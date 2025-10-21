package etl_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus"
	"github.com/qor5/go-que/pg"
	"github.com/qor5/x/v3/gormx"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/theplant/etl"
	"github.com/theplant/etl/pgtarget"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func TestIdentitySyncer(t *testing.T) {
	ctx := context.Background()

	// Setup test databases
	sourceDB, targetDB, pipelineSQLDB := setupTestDatabases(t, ctx)

	// Prepare test data in source database
	prepareSourceTestData(t, sourceDB)

	// Create and start pipeline with shorter interval for testing
	pipeline, err := etl.NewPipeline(&etl.PipelineConfig[*etl.Cursor]{
		Source: &identitySyncer{
			sourceDB: sourceDB,
			targetDB: targetDB,
		},
		QueueDB:                 pipelineSQLDB,
		QueueName:               "identity_system_etl",
		PageSize:                10,
		Interval:                3 * time.Second, // Shorter interval for faster testing
		ConsistencyDelay:        1 * time.Second, // Shorter delay for faster testing
		RetryPolicy:             bus.DefaultRetryPolicyFactory(),
		CircuitBreakerThreshold: 3,
		CircuitBreakerCooldown:  60 * time.Second,
	})
	require.NoError(t, err, "Failed to create pipeline")

	// Start ETL process
	controller, err := pipeline.Start(ctx, &etl.Cursor{})
	require.NoError(t, err, "Failed to start pipeline")
	defer func() { _ = controller.Stop(ctx) }()

	// Wait for ETL to complete
	time.Sleep(2 * time.Second)

	// Verify results in target database after first sync
	verifyTargetData(t, targetDB)
	t.Log("✅ First ETL sync completed successfully")

	// === Second Round: Test incremental soft delete → physical delete ===
	t.Log("🔄 Starting second round: soft delete existing user and sync")

	// Soft delete user1 (Alice)
	result := sourceDB.Where("id = ?", "user1").Delete(&User{})
	require.NoError(t, result.Error, "Failed to soft delete user1")
	require.Equal(t, int64(1), result.RowsAffected, "Expected to soft delete 1 user")

	// Soft delete ALL of Alice's credentials (simulating cascade delete behavior)
	result = sourceDB.Where("user_id = ?", "user1").Delete(&UserCred{})
	require.NoError(t, result.Error, "Failed to soft delete Alice's credentials")
	require.GreaterOrEqual(t, result.RowsAffected, int64(1), "Expected to soft delete at least 1 credential")

	t.Logf("   - Soft deleted Alice (user1) and all her credentials (%d records)", result.RowsAffected)

	// Wait for next ETL cycle (interval=3s) to process the soft deletes
	t.Log("   - Waiting for next ETL cycle to process soft deletes...")
	time.Sleep(5 * time.Second) // Wait for 5 seconds to ensure ETL runs (interval=3s + processing time)

	// Verify that Alice and cred2 are physically deleted from target database
	verifyTargetDataAfterDeletion(t, targetDB)

	t.Log("✅ Identity System many-to-many ETL test completed successfully")
}

// ====== Source Database Models (Legacy System) ======

type User struct {
	ID          string `gorm:"primaryKey"`
	Username    string
	Email       string
	DisplayName string
	Status      string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   gorm.DeletedAt // GORM standard soft delete field
	UserCreds   []*UserCred    `gorm:"foreignKey:UserID"` // One-to-many relationship with UserCreds
}

type UserCred struct {
	ID         string `gorm:"primaryKey"`
	UserID     string
	CredType   string // "password", "oauth", "api_key"
	Identifier string // email, oauth_provider, api_key_name
	CredValue  string // hashed_password, oauth_token, api_key
	IsActive   bool
	ExpiresAt  *time.Time
	User       *User `gorm:"foreignKey:UserID"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
	DeletedAt  gorm.DeletedAt // GORM standard soft delete field
}

// ====== Target Database Models (New Identity System) ======

type Identity struct {
	ID          string `gorm:"primaryKey"`
	Username    string
	Email       string
	DisplayName string
	Status      string // "active", "inactive", "suspended"
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Credential struct {
	ID         string `gorm:"primaryKey"`
	IdentityID string // FK to Identity
	Type       string // "password", "oauth", "api_key"
	Value      string // credential value
	IsActive   bool
	ExpiresAt  *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type CredentialIdentifier struct {
	ID           string `gorm:"primaryKey"`
	CredentialID string // FK to Credential
	Type         string // "email", "username", "oauth_provider", "api_key_name"
	Value        string // actual identifier value
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ====== Source Implementation for Identity System ETL ======

type identitySyncer struct {
	sourceDB *gorm.DB
	targetDB *gorm.DB
}

var _ etl.Source[*etl.Cursor] = (*identitySyncer)(nil)

func (s *identitySyncer) Extract(ctx context.Context, req *etl.ExtractRequest[*etl.Cursor]) (*etl.ExtractResponse[*etl.Cursor], error) {
	cursor := req.After

	// Use CTE to calculate max update time across all related tables for each user
	// This approach is more scalable and performant when dealing with multiple tables
	// Note: Query ALL records including soft-deleted ones for soft→physical delete conversion
	usersQuery := `
		WITH user_max_at AS (
			SELECT 
				u.id,
				u.updated_at as user_updated_at,
				GREATEST(
					u.updated_at,
					COALESCE(u.deleted_at, u.updated_at),
					COALESCE(MAX(uc.updated_at), u.updated_at),
					COALESCE(MAX(uc.deleted_at), u.updated_at)
				) as max_at
			FROM users u
			LEFT JOIN user_creds uc ON uc.user_id = u.id
			GROUP BY u.id
		)
		SELECT u.*, uma.max_at 
		FROM users u
		INNER JOIN user_max_at uma ON uma.id = u.id
		WHERE 1=1`

	args := []any{}

	// Apply cursor pagination using the calculated max_at
	if cursor != nil && (!cursor.At.IsZero() || cursor.ID != "") {
		usersQuery += ` AND (uma.max_at, u.id) > (?, ?)`
		args = append(args, cursor.At, cursor.ID)
	}

	// Add time window using the calculated max_at: [FromAt, BeforeAt)
	usersQuery += ` AND uma.max_at >= ? AND uma.max_at < ?`
	args = append(args, req.FromAt, req.BeforeAt)

	// Order by the calculated max_at for consistent pagination
	usersQuery += ` ORDER BY uma.max_at ASC, u.id ASC LIMIT ?`
	args = append(args, req.First+1)

	// Define a struct to capture the Raw query results including max_at
	type UserWithMaxAt struct {
		*User
		MaxAt time.Time `gorm:"column:max_at"`
	}

	// Execute CTE query with automatic relationship loading (including soft-deleted records)
	var users []*UserWithMaxAt
	if err := s.sourceDB.WithContext(ctx).
		Unscoped(). // Include soft-deleted records in both User and UserCreds
		Raw(usersQuery, args...).
		Preload("UserCreds"). // Load ALL UserCreds including soft-deleted ones
		Find(&users).Error; err != nil {
		return nil, err
	}

	if len(users) == 0 {
		return &etl.ExtractResponse[*etl.Cursor]{Target: nil}, nil
	}

	// Check for next page
	hasNextPage := len(users) > req.First
	if hasNextPage {
		users = users[:req.First]
	}

	// Create end cursor
	last := users[len(users)-1]
	endCursor := &etl.Cursor{
		At: last.MaxAt, // Use pre-calculated max_at from CTE
		ID: last.ID,    // Use User's ID for cursor (consistent identifier)
	}

	// Transform to target tables
	datas, err := s.transform(ctx, lo.Map(users, func(user *UserWithMaxAt, _ int) *User {
		return user.User
	}))
	if err != nil {
		return nil, err
	}

	// Create target
	target, err := pgtarget.New(s.targetDB, req, datas, s.commit)
	if err != nil {
		return nil, err
	}
	// Add metadata column hook to store soft delete information
	target = target.WithCreateStagingTableHook(pgtarget.AddMetadataColumnHook[*etl.Cursor])

	return &etl.ExtractResponse[*etl.Cursor]{
		Target:      target,
		EndCursor:   endCursor,
		HasNextPage: hasNextPage,
	}, nil
}

func (s *identitySyncer) transform(_ context.Context, users []*User) (etl.TargetDatas, error) {
	type metadata struct {
		DeletedAt gorm.DeletedAt `json:"deletedAt"`
	}

	// Transform to MULTIPLE TARGET TABLES (process all records, handle soft delete in commit)
	type identityWithMetadata struct {
		*Identity
		Metadata datatypes.JSONType[*metadata] `gorm:"column:__etl_metadata__"`
	}
	type credentialWithMetadata struct {
		*Credential
		Metadata datatypes.JSONType[*metadata] `gorm:"column:__etl_metadata__"`
	}
	type credentialIdentifierWithMetadata struct {
		*CredentialIdentifier
		Metadata datatypes.JSONType[*metadata] `gorm:"column:__etl_metadata__"`
	}
	var identities []*identityWithMetadata
	var credentials []*credentialWithMetadata
	var credentialIdentifiers []*credentialIdentifierWithMetadata

	// Since we're paginating by User, each User appears only once
	for _, user := range users {
		// 1. Create Identity from User data (preserving original UpdatedAt)
		identity := &Identity{
			ID:          user.ID,
			Username:    user.Username,
			Email:       user.Email,
			DisplayName: user.DisplayName,
			Status:      user.Status,
			CreatedAt:   user.CreatedAt,
			UpdatedAt:   user.UpdatedAt, // Preserve original User UpdatedAt
		}
		identities = append(identities, &identityWithMetadata{
			Identity: identity,
			Metadata: datatypes.NewJSONType(&metadata{
				DeletedAt: user.DeletedAt,
			}),
		})

		// 2. Process all UserCreds for this User
		for _, userCred := range user.UserCreds {
			// Create Credential (one-to-one with UserCred, preserving original UpdatedAt)
			credential := &Credential{
				ID:         userCred.ID, // Use same ID as source UserCred
				IdentityID: userCred.UserID,
				Type:       userCred.CredType,
				Value:      userCred.CredValue,
				IsActive:   userCred.IsActive,
				ExpiresAt:  userCred.ExpiresAt,
				CreatedAt:  userCred.CreatedAt,
				UpdatedAt:  userCred.UpdatedAt, // Preserve original UserCred UpdatedAt
			}
			credentials = append(credentials, &credentialWithMetadata{
				Credential: credential,
				Metadata: datatypes.NewJSONType(&metadata{
					DeletedAt: userCred.DeletedAt,
				}),
			})

			// 3. Create CredentialIdentifier (extracted from UserCred.Identifier)
			identifierType := ""
			switch userCred.CredType {
			case "password":
				identifierType = "email" // Password auth uses email as identifier
			case "oauth":
				identifierType = "oauth_provider" // OAuth uses provider as identifier
			case "api_key":
				identifierType = "api_key_name" // API key uses name as identifier
			default:
				identifierType = "unknown"
			}

			credentialIdentifier := &CredentialIdentifier{
				ID:           userCred.ID + "_identifier", // Generate unique ID
				CredentialID: userCred.ID,
				Type:         identifierType,
				Value:        userCred.Identifier,
				CreatedAt:    userCred.CreatedAt,
				UpdatedAt:    userCred.UpdatedAt, // Preserve original UserCred UpdatedAt
			}
			credentialIdentifiers = append(credentialIdentifiers, &credentialIdentifierWithMetadata{
				CredentialIdentifier: credentialIdentifier,
				Metadata: datatypes.NewJSONType(&metadata{
					DeletedAt: userCred.DeletedAt,
				}),
			})
		}
	}

	// TODO: Consider filtering unchanged records early to reduce unnecessary inserts (e.g., when only credentials changed but identities didn't)
	// Create target data for MULTIPLE TABLES
	return etl.TargetDatas{
		{Table: "identities", Records: identities},
		{Table: "credentials", Records: credentials},
		{Table: "credential_identifiers", Records: credentialIdentifiers},
	}, nil
}

func (s *identitySyncer) commit(ctx context.Context, input *pgtarget.CommitInput[*etl.Cursor]) (*pgtarget.CommitOutput[*etl.Cursor], error) {
	query := `
			MERGE INTO identities AS t
					USING ` + input.StagingTables["identities"] + ` AS s
					ON t.id = s.id
					WHEN MATCHED AND s.__etl_metadata__->>'deletedAt' IS NOT NULL THEN
						DELETE
					WHEN MATCHED AND s.__etl_metadata__->>'deletedAt' IS NULL AND s.updated_at > t.updated_at THEN
						UPDATE SET
							username = s.username,
							email = s.email,
							display_name = s.display_name,
							status = s.status,
							updated_at = s.updated_at
					WHEN NOT MATCHED AND s.__etl_metadata__->>'deletedAt' IS NULL THEN
						INSERT (id, username, email, display_name, status, created_at, updated_at)
						VALUES (s.id, s.username, s.email, s.display_name, s.status, s.created_at, s.updated_at);

			MERGE INTO credentials AS t
					USING ` + input.StagingTables["credentials"] + ` AS s
					ON t.id = s.id
					WHEN MATCHED AND s.__etl_metadata__->>'deletedAt' IS NOT NULL THEN
						DELETE
					WHEN MATCHED AND s.__etl_metadata__->>'deletedAt' IS NULL AND s.updated_at > t.updated_at THEN
						UPDATE SET
							identity_id = s.identity_id,
							type = s.type,
							value = s.value,
							is_active = s.is_active,
							expires_at = s.expires_at,
							updated_at = s.updated_at
					WHEN NOT MATCHED AND s.__etl_metadata__->>'deletedAt' IS NULL THEN
						INSERT (id, identity_id, type, value, is_active, expires_at, created_at, updated_at)
						VALUES (s.id, s.identity_id, s.type, s.value, s.is_active, s.expires_at, s.created_at, s.updated_at);

			MERGE INTO credential_identifiers AS t
					USING ` + input.StagingTables["credential_identifiers"] + ` AS s
					ON t.id = s.id
					WHEN MATCHED AND s.__etl_metadata__->>'deletedAt' IS NOT NULL THEN
						DELETE
					WHEN MATCHED AND s.__etl_metadata__->>'deletedAt' IS NULL AND s.updated_at > t.updated_at THEN
						UPDATE SET
							credential_id = s.credential_id,
							type = s.type,
							value = s.value,
							updated_at = s.updated_at
					WHEN NOT MATCHED AND s.__etl_metadata__->>'deletedAt' IS NULL THEN
						INSERT (id, credential_id, type, value, created_at, updated_at)
						VALUES (s.id, s.credential_id, s.type, s.value, s.created_at, s.updated_at);
					
		`

	if err := input.DB().WithContext(ctx).Exec(query).Error; err != nil {
		return nil, errors.Wrapf(err, "failed to execute commit query")
	}

	return &pgtarget.CommitOutput[*etl.Cursor]{}, nil
}

func setupTestDatabases(t *testing.T, ctx context.Context) (*gorm.DB, *gorm.DB, *sql.DB) {
	sourceSuite := gormx.MustStartTestSuite(ctx)
	t.Cleanup(func() { _ = sourceSuite.Stop(context.Background()) })
	t.Logf("SourceDB: %s", sourceSuite.DSN())

	sourceDB := sourceSuite.DB()
	require.NoError(t, sourceDB.AutoMigrate(&User{}, &UserCred{}), "Failed to migrate source database")

	// Setup target database
	targetSuite := gormx.MustStartTestSuite(ctx)
	t.Cleanup(func() { _ = targetSuite.Stop(context.Background()) })
	t.Logf("TargetDB: %s", targetSuite.DSN())

	targetDB := targetSuite.DB()
	require.NoError(t, targetDB.AutoMigrate(&Identity{}, &Credential{}, &CredentialIdentifier{}), "Failed to migrate target database")

	pipelineSuite := gormx.MustStartTestSuite(ctx)
	t.Cleanup(func() { _ = pipelineSuite.Stop(context.Background()) })
	t.Logf("PipelineDB: %s", pipelineSuite.DSN())

	pipelineSQLDB, err := pipelineSuite.DB().DB()
	require.NoError(t, err, "Failed to get sql.DB from pipeline database")
	require.NoError(t, pg.Migrate(pipelineSQLDB), "Failed to migrate pipeline database")

	return sourceDB, targetDB, pipelineSQLDB
}

func prepareSourceTestData(t *testing.T, db *gorm.DB) {
	now := time.Now()

	// Create users (source table 1)
	users := []User{
		{
			ID:          "user1",
			Username:    "alice",
			Email:       "alice@example.com",
			DisplayName: "Alice Johnson",
			Status:      "active",
			CreatedAt:   now.AddDate(0, 0, -2),
			UpdatedAt:   now.AddDate(0, 0, -1),
		},
		{
			ID:          "user2",
			Username:    "bob",
			Email:       "bob@example.com",
			DisplayName: "Bob Smith",
			Status:      "active",
			CreatedAt:   now.AddDate(0, 0, -2),
			UpdatedAt:   now.AddDate(0, 0, -1),
		},
		{
			ID:          "user3",
			Username:    "charlie",
			Email:       "charlie@example.com",
			DisplayName: "Charlie Wilson",
			Status:      "inactive",
			CreatedAt:   now.AddDate(0, 0, -2),
			UpdatedAt:   now.AddDate(0, 0, -1),
		},
	}
	require.NoError(t, db.Create(&users).Error, "Failed to create users")

	// Create user credentials (source table 2) - multiple credentials per user
	expiresAt := now.AddDate(1, 0, 0) // Expires in 1 year
	userCreds := []UserCred{
		// Alice's credentials
		{
			ID:         "cred1",
			UserID:     "user1",
			CredType:   "password",
			Identifier: "alice@example.com",
			CredValue:  "hashed_password_alice",
			IsActive:   true,
			CreatedAt:  now.AddDate(0, 0, -2),
			UpdatedAt:  now.AddDate(0, 0, -1),
		},
		{
			ID:         "cred2",
			UserID:     "user1",
			CredType:   "oauth",
			Identifier: "google",
			CredValue:  "oauth_token_alice_google",
			IsActive:   true,
			CreatedAt:  now.AddDate(0, 0, -2),
			UpdatedAt:  now.AddDate(0, 0, -1),
		},
		// Bob's credentials
		{
			ID:         "cred3",
			UserID:     "user2",
			CredType:   "password",
			Identifier: "bob@example.com",
			CredValue:  "hashed_password_bob",
			IsActive:   true,
			CreatedAt:  now.AddDate(0, 0, -2),
			UpdatedAt:  now.AddDate(0, 0, -1),
		},
		{
			ID:         "cred4",
			UserID:     "user2",
			CredType:   "api_key",
			Identifier: "bob_api_key",
			CredValue:  "sk-1234567890abcdef",
			IsActive:   true,
			ExpiresAt:  &expiresAt,
			CreatedAt:  now.AddDate(0, 0, -2),
			UpdatedAt:  now.AddDate(0, 0, -1),
		},
		// Charlie's credentials (inactive user)
		{
			ID:         "cred5",
			UserID:     "user3",
			CredType:   "password",
			Identifier: "charlie@example.com",
			CredValue:  "hashed_password_charlie",
			IsActive:   false, // Inactive credential for inactive user
			CreatedAt:  now.AddDate(0, 0, -2),
			UpdatedAt:  now.AddDate(0, 0, -1),
		},
	}
	require.NoError(t, db.Create(&userCreds).Error, "Failed to create user credentials")

	// Create some soft-deleted data to test filtering
	deletedAt := now.AddDate(0, 0, -5) // Deleted 5 days ago
	deletedUsers := []User{
		{
			ID:          "user_deleted_1",
			Username:    "deleted_dave",
			Email:       "dave@example.com",
			DisplayName: "Dave (Deleted)",
			Status:      "inactive",
			CreatedAt:   now.AddDate(0, 0, -10),
			UpdatedAt:   now.AddDate(0, 0, -6),
			DeletedAt:   gorm.DeletedAt{Time: deletedAt, Valid: true}, // GORM soft delete
		},
		{
			ID:          "user_deleted_2",
			Username:    "deleted_eve",
			Email:       "eve@example.com",
			DisplayName: "Eve (Deleted)",
			Status:      "inactive",
			CreatedAt:   now.AddDate(0, 0, -8),
			UpdatedAt:   now.AddDate(0, 0, -4),
			DeletedAt:   gorm.DeletedAt{Time: deletedAt, Valid: true}, // GORM soft delete
		},
	}
	require.NoError(t, db.Create(&deletedUsers).Error, "Failed to create deleted users")

	deletedUserCreds := []UserCred{
		{
			ID:         "cred_deleted_1",
			UserID:     "user_deleted_1",
			CredType:   "password",
			Identifier: "dave@example.com",
			CredValue:  "hashed_password_dave",
			IsActive:   false,
			CreatedAt:  now.AddDate(0, 0, -10),
			UpdatedAt:  now.AddDate(0, 0, -6),
			DeletedAt:  gorm.DeletedAt{Time: deletedAt, Valid: true}, // GORM soft delete
		},
		{
			ID:         "cred_deleted_2",
			UserID:     "user1", // Alice's deleted credential
			CredType:   "old_password",
			Identifier: "alice@example.com",
			CredValue:  "old_hashed_password_alice",
			IsActive:   false,
			CreatedAt:  now.AddDate(0, 0, -15),
			UpdatedAt:  now.AddDate(0, 0, -10),
			DeletedAt:  gorm.DeletedAt{Time: deletedAt, Valid: true}, // GORM soft delete
		},
	}
	require.NoError(t, db.Create(&deletedUserCreds).Error, "Failed to create deleted user credentials")

	t.Log("✅ Identity system source test data prepared successfully")
	t.Logf("   - Users: %d (+ %d soft deleted)", len(users), len(deletedUsers))
	t.Logf("   - UserCreds: %d (+ %d soft deleted)", len(userCreds), len(deletedUserCreds))
	t.Log("   Data structure:")
	t.Log("     - Alice: password + oauth (google) [+ 1 deleted old_password]")
	t.Log("     - Bob: password + api_key (with expiration)")
	t.Log("     - Charlie: password (inactive)")
	t.Log("     - Dave & Eve: soft-deleted users (should be filtered out)")
}

func verifyTargetData(t *testing.T, db *gorm.DB) {
	// Verify TARGET TABLE 1: identities
	var identities []Identity
	require.NoError(t, db.Find(&identities).Error, "Failed to query identities")

	expectedIdentities := 3 // Alice, Bob, Charlie
	assert.Len(t, identities, expectedIdentities, "Expected %d identities, got %d", expectedIdentities, len(identities))

	// Check identity details
	identityMap := make(map[string]Identity)
	for _, identity := range identities {
		identityMap[identity.ID] = identity
		t.Logf("   - Identity %s (%s): %s <%s> [%s]",
			identity.ID, identity.Username, identity.DisplayName, identity.Email, identity.Status)
	}

	// Verify Alice's identity
	alice, exists := identityMap["user1"]
	require.True(t, exists, "Alice identity should exist")
	assert.Equal(t, "alice", alice.Username, "Alice username should match")
	assert.Equal(t, "alice@example.com", alice.Email, "Alice email should match")
	assert.Equal(t, "active", alice.Status, "Alice status should be active")

	// Verify TARGET TABLE 2: credentials
	var credentials []Credential
	require.NoError(t, db.Find(&credentials).Error, "Failed to query credentials")

	expectedCredentials := 5 // 2 for Alice, 2 for Bob, 1 for Charlie
	assert.Len(t, credentials, expectedCredentials, "Expected %d credentials, got %d", expectedCredentials, len(credentials))

	// Check credential details and group by identity
	credentialsByIdentity := make(map[string][]Credential)
	for _, credential := range credentials {
		credentialsByIdentity[credential.IdentityID] = append(credentialsByIdentity[credential.IdentityID], credential)
		t.Logf("   - Credential %s (%s): type=%s, active=%v, expires=%v",
			credential.ID, credential.IdentityID, credential.Type, credential.IsActive, credential.ExpiresAt)
	}

	// Alice should have 2 credentials (password + oauth)
	aliceCredentials := credentialsByIdentity["user1"]
	assert.Len(t, aliceCredentials, 2, "Alice should have 2 credentials")

	// Bob should have 2 credentials (password + api_key)
	bobCredentials := credentialsByIdentity["user2"]
	assert.Len(t, bobCredentials, 2, "Bob should have 2 credentials")

	// Verify TARGET TABLE 3: credential identifiers
	var credentialIdentifiers []CredentialIdentifier
	require.NoError(t, db.Find(&credentialIdentifiers).Error, "Failed to query credential identifiers")

	expectedIdentifiers := 5 // One identifier per credential
	assert.Len(t, credentialIdentifiers, expectedIdentifiers, "Expected %d credential identifiers", expectedIdentifiers)

	// Check credential identifier details
	identifiersByCredential := make(map[string][]CredentialIdentifier)
	for _, identifier := range credentialIdentifiers {
		identifiersByCredential[identifier.CredentialID] = append(identifiersByCredential[identifier.CredentialID], identifier)
		t.Logf("   - Identifier %s (cred:%s): type=%s, value=%s",
			identifier.ID, identifier.CredentialID, identifier.Type, identifier.Value)
	}

	// Verify specific identifier types
	emailIdentifierCount := 0
	oauthIdentifierCount := 0
	apiKeyIdentifierCount := 0

	for _, identifier := range credentialIdentifiers {
		switch identifier.Type {
		case "email":
			emailIdentifierCount++
		case "oauth_provider":
			oauthIdentifierCount++
		case "api_key_name":
			apiKeyIdentifierCount++
		}
	}

	// Verify identifier type counts
	assert.Equal(t, 3, emailIdentifierCount, "Expected 3 email identifiers (password credentials for Alice, Bob, Charlie)")
	assert.Equal(t, 1, oauthIdentifierCount, "Expected 1 oauth_provider identifier (OAuth credential for Alice)")
	assert.Equal(t, 1, apiKeyIdentifierCount, "Expected 1 api_key_name identifier (API key credential for Bob)")

	t.Log("✅ Identity system target data verification completed successfully")
	t.Logf("   - Identities: %d records", len(identities))
	t.Logf("   - Credentials: %d records", len(credentials))
	t.Logf("   - Credential Identifiers: %d records", len(credentialIdentifiers))
	t.Log("   Verification details:")
	t.Logf("     - Email identifiers: %d", emailIdentifierCount)
	t.Logf("     - OAuth identifiers: %d", oauthIdentifierCount)
	t.Logf("     - API key identifiers: %d", apiKeyIdentifierCount)
}

// verifyTargetDataAfterDeletion verifies that Alice and her credentials are physically deleted
func verifyTargetDataAfterDeletion(t *testing.T, targetDB *gorm.DB) {
	// Verify Alice (user1) is physically deleted
	var identity Identity
	err := targetDB.Where("id = ?", "user1").First(&identity).Error
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound, "Alice (user1) should be physically deleted from identities")

	// Verify Alice's credentials are physically deleted
	var credential Credential
	err = targetDB.Where("id IN (?)", []string{"cred1", "cred2"}).First(&credential).Error
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound, "Alice's credentials (cred1, cred2) should be physically deleted")

	// Verify Alice's credential identifiers are physically deleted
	var identifier CredentialIdentifier
	err = targetDB.Where("id IN (?)", []string{"cred1_identifier", "cred2_identifier"}).First(&identifier).Error
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound, "Alice's credential identifiers should be physically deleted")

	t.Log("✅ Verified: Alice and all her related data have been physically deleted")
}
