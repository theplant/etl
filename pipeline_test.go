package etl_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/theplant/etl"
	"github.com/theplant/etl/bqtarget"
	"github.com/theplant/etl/pgtarget"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
	"github.com/qor5/go-bus"
	"github.com/qor5/go-que/pg"
	"github.com/qor5/x/v3/gormx"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
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
	target, err := pgtarget.New(&pgtarget.Config[*etl.Cursor]{
		DB:               s.targetDB,
		Req:              req,
		Datas:            datas,
		CommitFunc:       s.commit,
		UseUnloggedTable: false, // The default is to use the temp table.
	})
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

	if err := input.DB.WithContext(ctx).Exec(query).Error; err != nil {
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

// BigQuery table models - matching the Go models
// Note: These types include __etl_metadata__ field for ETL tracking and use bigquery.NullTimestamp
// for nullable timestamp fields to avoid BigQuery Inserter type inference issues.
type BQIdentity struct {
	ID          string    `bigquery:"id"`
	Username    string    `bigquery:"username"`
	Email       string    `bigquery:"email"`
	DisplayName string    `bigquery:"display_name"`
	Status      string    `bigquery:"status"`
	CreatedAt   time.Time `bigquery:"created_at"`
	UpdatedAt   time.Time `bigquery:"updated_at"`
	Metadata    string    `bigquery:"__etl_metadata__"`
}

type BQCredential struct {
	ID         string                 `bigquery:"id"`
	IdentityID string                 `bigquery:"identity_id"`
	Type       string                 `bigquery:"type"`
	Value      string                 `bigquery:"value"`
	IsActive   bool                   `bigquery:"is_active"`
	ExpiresAt  bigquery.NullTimestamp `bigquery:"expires_at"` // Use NullTimestamp instead of *time.Time for BigQuery Inserter compatibility
	CreatedAt  time.Time              `bigquery:"created_at"`
	UpdatedAt  time.Time              `bigquery:"updated_at"`
	Metadata   string                 `bigquery:"__etl_metadata__"`
}

type BQCredentialIdentifier struct {
	ID           string    `bigquery:"id"`
	CredentialID string    `bigquery:"credential_id"`
	Type         string    `bigquery:"type"`
	Value        string    `bigquery:"value"`
	CreatedAt    time.Time `bigquery:"created_at"`
	UpdatedAt    time.Time `bigquery:"updated_at"`
	Metadata     string    `bigquery:"__etl_metadata__"`
}

func TestPipeline_BQTarget(t *testing.T) {
	if os.Getenv("RUN_BIGQUERY_TESTS") != "true" {
		t.Skipf("Skipping BigQuery Test: %s", t.Name())
		return
	}
	ctx := context.Background()

	// Setup source database and pipeline database
	sourceDB, pipelineSQLDB := setupTestDatabasesForBQ(t, ctx)

	// Prepare test data in source database
	prepareSourceTestData(t, sourceDB)

	// Setup BigQuery dataset and tables
	client, dataset := setupBQTables(t, ctx, "product-data-sandbox", "etl_test")

	// Create and start pipeline with shorter interval for testing
	pipeline, err := etl.NewPipeline(&etl.PipelineConfig[*etl.Cursor]{
		Source: &identityBQSyncer{
			sourceDB: sourceDB,
			client:   client,
			dataset:  dataset,
		},
		QueueDB:                 pipelineSQLDB,
		QueueName:               "bigquery_etl",
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
	// BigQuery may have eventual consistency, so we wait for data to be available
	time.Sleep(60 * time.Second)

	// Verify results in BigQuery after this sync
	verifyBigQueryTargetData(t, ctx, client, dataset)
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

	// Wait for next ETL cycle to process the soft deletes
	// BigQuery may have eventual consistency, so we wait for data to be available
	t.Log("   - Waiting for next ETL cycle to process soft deletes...")
	time.Sleep(60 * time.Second)

	// Verify that Alice and her credentials are physically deleted from BigQuery
	verifyBigQueryTargetDataAfterDeletion(t, ctx, client, dataset)

	t.Log("✅ BigQuery ETL test completed successfully")
}

// ====== Source Implementation for BigQuery ETL ======

type identityBQSyncer struct {
	sourceDB *gorm.DB
	client   *bigquery.Client
	dataset  *bigquery.Dataset
}

var _ etl.Source[*etl.Cursor] = (*identityBQSyncer)(nil)

func (s *identityBQSyncer) Extract(ctx context.Context, req *etl.ExtractRequest[*etl.Cursor]) (*etl.ExtractResponse[*etl.Cursor], error) {
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

	// Create BigQuery target
	target, err := bqtarget.New(&bqtarget.Config[*etl.Cursor]{
		Client:     s.client,
		DatasetID:  s.dataset.DatasetID,
		Req:        req,
		Datas:      datas,
		CommitFunc: s.commit,
	})
	if err != nil {
		return nil, err
	}

	// Add metadata column hook
	target.WithCreateStagingTableHook(bqtarget.AddMetadataColumnHook[*etl.Cursor])

	return &etl.ExtractResponse[*etl.Cursor]{
		Target:      target,
		EndCursor:   endCursor,
		HasNextPage: hasNextPage,
	}, nil
}

func (s *identityBQSyncer) transform(_ context.Context, users []*User) (etl.TargetDatas, error) {
	type metadata struct {
		DeletedAt gorm.DeletedAt `json:"deletedAt"`
	}

	// Transform to BigQuery models with metadata
	// Note: BigQuery JSON column requires string type, not json.RawMessage (which is treated as BYTES)
	// We use the original BQ* types directly which include Metadata field and use bigquery.NullTimestamp

	var identities []*BQIdentity
	var credentials []*BQCredential
	var credentialIdentifiers []*BQCredentialIdentifier

	// Since we're paginating by User, each User appears only once
	for _, user := range users {
		// 1. Create Identity from User data (preserving original UpdatedAt)
		identity := &BQIdentity{
			ID:          user.ID,
			Username:    user.Username,
			Email:       user.Email,
			DisplayName: user.DisplayName,
			Status:      user.Status,
			CreatedAt:   user.CreatedAt,
			UpdatedAt:   user.UpdatedAt,
		}
		metadataBytes, _ := json.Marshal(&metadata{DeletedAt: user.DeletedAt})
		identity.Metadata = string(metadataBytes) // Convert to string for BigQuery JSON column
		identities = append(identities, identity)

		// 2. Process all UserCreds for this User
		for _, userCred := range user.UserCreds {
			// Create Credential (one-to-one with UserCred, preserving original UpdatedAt)
			credential := &BQCredential{
				ID:         userCred.ID,
				IdentityID: userCred.UserID,
				Type:       userCred.CredType,
				Value:      userCred.CredValue,
				IsActive:   userCred.IsActive,
				CreatedAt:  userCred.CreatedAt,
				UpdatedAt:  userCred.UpdatedAt,
			}

			// Convert *time.Time to bigquery.NullTimestamp
			if userCred.ExpiresAt != nil {
				credential.ExpiresAt = bigquery.NullTimestamp{Timestamp: *userCred.ExpiresAt, Valid: true}
			}
			// If userCred.ExpiresAt is nil, credential.ExpiresAt will be zero value (Valid: false)

			credMetadataBytes, _ := json.Marshal(&metadata{DeletedAt: userCred.DeletedAt})

			credential.Metadata = string(credMetadataBytes) // Convert to string for BigQuery JSON column
			credentials = append(credentials, credential)

			// 3. Create CredentialIdentifier (extracted from UserCred.Identifier)
			identifierType := ""
			switch userCred.CredType {
			case "password":
				identifierType = "email"
			case "oauth":
				identifierType = "oauth_provider"
			case "api_key":
				identifierType = "api_key_name"
			default:
				identifierType = "unknown"
			}

			credentialIdentifier := &BQCredentialIdentifier{
				ID:           userCred.ID + "_identifier",
				CredentialID: userCred.ID,
				Type:         identifierType,
				Value:        userCred.Identifier,
				CreatedAt:    userCred.CreatedAt,
				UpdatedAt:    userCred.UpdatedAt,
			}
			identifierMetadataBytes, _ := json.Marshal(&metadata{DeletedAt: userCred.DeletedAt})
			credentialIdentifier.Metadata = string(identifierMetadataBytes) // Convert to string for BigQuery JSON column
			credentialIdentifiers = append(credentialIdentifiers, credentialIdentifier)
		}
	}

	// Create target data for MULTIPLE TABLES
	return etl.TargetDatas{
		{Table: "identities", Records: identities},
		{Table: "credentials", Records: credentials},
		{Table: "credential_identifiers", Records: credentialIdentifiers},
	}, nil
}

func (s *identityBQSyncer) commit(ctx context.Context, input *bqtarget.CommitInput[*etl.Cursor]) (*bqtarget.CommitOutput[*etl.Cursor], error) {
	client := input.Client
	projectID := client.Project()
	datasetID := input.DatasetID

	identitiesStagingTable := input.StagingTables["identities"]
	credentialsStagingTable := input.StagingTables["credentials"]
	credentialIdentifiersStagingTable := input.StagingTables["credential_identifiers"]

	// Build multi-statement transaction query
	multiStatementQuery := fmt.Sprintf(`
		BEGIN TRANSACTION;

		MERGE `+"`%s.%s.identities`"+` AS t
		USING `+"`%s.%s.%s`"+` AS s
		ON t.id = s.id
		WHEN MATCHED AND JSON_EXTRACT_SCALAR(s.__etl_metadata__, '$.deletedAt') IS NOT NULL THEN
			DELETE
		WHEN MATCHED AND JSON_EXTRACT_SCALAR(s.__etl_metadata__, '$.deletedAt') IS NULL AND s.updated_at > t.updated_at THEN
			UPDATE SET
				username = s.username,
				email = s.email,
				display_name = s.display_name,
				status = s.status,
				updated_at = s.updated_at
		WHEN NOT MATCHED AND JSON_EXTRACT_SCALAR(s.__etl_metadata__, '$.deletedAt') IS NULL THEN
			INSERT (id, username, email, display_name, status, created_at, updated_at)
			VALUES (s.id, s.username, s.email, s.display_name, s.status, s.created_at, s.updated_at);

		MERGE `+"`%s.%s.credentials`"+` AS t
		USING `+"`%s.%s.%s`"+` AS s
		ON t.id = s.id
		WHEN MATCHED AND JSON_EXTRACT_SCALAR(s.__etl_metadata__, '$.deletedAt') IS NOT NULL THEN
			DELETE
		WHEN MATCHED AND JSON_EXTRACT_SCALAR(s.__etl_metadata__, '$.deletedAt') IS NULL AND s.updated_at > t.updated_at THEN
			UPDATE SET
				identity_id = s.identity_id,
				type = s.type,
				value = s.value,
				is_active = s.is_active,
				expires_at = s.expires_at,
				updated_at = s.updated_at
		WHEN NOT MATCHED AND JSON_EXTRACT_SCALAR(s.__etl_metadata__, '$.deletedAt') IS NULL THEN
			INSERT (id, identity_id, type, value, is_active, expires_at, created_at, updated_at)
			VALUES (s.id, s.identity_id, s.type, s.value, s.is_active, s.expires_at, s.created_at, s.updated_at);

		MERGE `+"`%s.%s.credential_identifiers`"+` AS t
		USING `+"`%s.%s.%s`"+` AS s
		ON t.id = s.id
		WHEN MATCHED AND JSON_EXTRACT_SCALAR(s.__etl_metadata__, '$.deletedAt') IS NOT NULL THEN
			DELETE
		WHEN MATCHED AND JSON_EXTRACT_SCALAR(s.__etl_metadata__, '$.deletedAt') IS NULL AND s.updated_at > t.updated_at THEN
			UPDATE SET
				credential_id = s.credential_id,
				type = s.type,
				value = s.value,
				updated_at = s.updated_at
		WHEN NOT MATCHED AND JSON_EXTRACT_SCALAR(s.__etl_metadata__, '$.deletedAt') IS NULL THEN
			INSERT (id, credential_id, type, value, created_at, updated_at)
			VALUES (s.id, s.credential_id, s.type, s.value, s.created_at, s.updated_at);

		COMMIT TRANSACTION;
	`,
		projectID, datasetID, projectID, datasetID, identitiesStagingTable,
		projectID, datasetID, projectID, datasetID, credentialsStagingTable,
		projectID, datasetID, projectID, datasetID, credentialIdentifiersStagingTable)

	// Execute multi-statement transaction
	// BigQuery supports multi-statement transactions when the query contains BEGIN TRANSACTION and COMMIT TRANSACTION
	q := client.Query(multiStatementQuery)
	job, err := q.Run(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to run multi-statement transaction")
	}

	// Wait for job to complete
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to wait for multi-statement transaction job")
	}

	if status.Err() != nil {
		// Get detailed error information
		errorMsg := status.Err().Error()
		return nil, errors.Wrapf(status.Err(), "multi-statement transaction failed: %s", errorMsg)
	}

	// Check if MERGE actually processed any rows
	// Note: BigQuery DML statistics are available in job statistics
	// For now, we assume success if no error occurred

	return &bqtarget.CommitOutput[*etl.Cursor]{}, nil
}

// verifyBigQueryTargetData verifies the data in BigQuery after ETL sync
// It verifies all records and all fields to ensure data integrity
func verifyBigQueryTargetData(t *testing.T, ctx context.Context, client *bigquery.Client, dataset *bigquery.Dataset) {
	projectID := client.Project()
	datasetID := dataset.DatasetID

	// Query all identities with all fields
	identitiesQuery := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.identities` ORDER BY id", projectID, datasetID))
	identitiesIt, err := identitiesQuery.Read(ctx)
	require.NoError(t, err, "Failed to query identities from BigQuery")

	var allIdentities []BQIdentity
	for {
		var identity BQIdentity
		err := identitiesIt.Next(&identity)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err)
		allIdentities = append(allIdentities, identity)
	}

	// Verify all identities: should have Alice (user1), Bob (user2), and Charlie (user3)
	// user_deleted_1 and user_deleted_2 should NOT exist (they have DeletedAt from start)
	expectedIdentities := map[string]BQIdentity{
		"user1": {
			ID:          "user1",
			Username:    "alice",
			Email:       "alice@example.com",
			DisplayName: "Alice Johnson",
			Status:      "active",
		},
		"user2": {
			ID:          "user2",
			Username:    "bob",
			Email:       "bob@example.com",
			DisplayName: "Bob Smith",
			Status:      "active",
		},
		"user3": {
			ID:          "user3",
			Username:    "charlie",
			Email:       "charlie@example.com",
			DisplayName: "Charlie Wilson",
			Status:      "inactive",
		},
	}

	assert.Len(t, allIdentities, 3, "Expected 3 identities (Alice, Bob, Charlie), got %d", len(allIdentities))

	for _, identity := range allIdentities {
		// Verify deleted users are not in the list
		assert.NotEqual(t, "user_deleted_1", identity.ID, "user_deleted_1 should not exist (was soft deleted from start)")
		assert.NotEqual(t, "user_deleted_2", identity.ID, "user_deleted_2 should not exist (was soft deleted from start)")

		// Verify all fields for all identities
		expected, exists := expectedIdentities[identity.ID]
		require.True(t, exists, "Unexpected identity ID: %s", identity.ID)
		assert.Equal(t, expected.Username, identity.Username, "Username mismatch for identity %s", identity.ID)
		assert.Equal(t, expected.Email, identity.Email, "Email mismatch for identity %s", identity.ID)
		assert.Equal(t, expected.DisplayName, identity.DisplayName, "DisplayName mismatch for identity %s", identity.ID)
		assert.Equal(t, expected.Status, identity.Status, "Status mismatch for identity %s", identity.ID)
		assert.False(t, identity.CreatedAt.IsZero(), "CreatedAt should not be zero for identity %s", identity.ID)
		assert.False(t, identity.UpdatedAt.IsZero(), "UpdatedAt should not be zero for identity %s", identity.ID)
		t.Logf("   - Verified identity %s (%s): %s <%s> [%s]",
			identity.ID, identity.Username, identity.DisplayName, identity.Email, identity.Status)
	}

	// Query all credentials with all fields
	credentialsQuery := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.credentials` ORDER BY id", projectID, datasetID))
	credentialsIt, err := credentialsQuery.Read(ctx)
	require.NoError(t, err, "Failed to query credentials from BigQuery")

	var allCredentials []BQCredential
	for {
		var credential BQCredential
		err := credentialsIt.Next(&credential)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err)
		allCredentials = append(allCredentials, credential)
	}

	// Verify all credentials: should have Alice's (cred1, cred2), Bob's (cred3, cred4), and Charlie's (cred5)
	// cred_deleted_1 and cred_deleted_2 should NOT exist (they have DeletedAt from start)
	expectedCredentials := map[string]BQCredential{
		"cred1": {
			ID:         "cred1",
			IdentityID: "user1",
			Type:       "password",
			IsActive:   true,
		},
		"cred2": {
			ID:         "cred2",
			IdentityID: "user1",
			Type:       "oauth",
			IsActive:   true,
		},
		"cred3": {
			ID:         "cred3",
			IdentityID: "user2",
			Type:       "password",
			IsActive:   true,
		},
		"cred4": {
			ID:         "cred4",
			IdentityID: "user2",
			Type:       "api_key",
			IsActive:   true,
		},
		"cred5": {
			ID:         "cred5",
			IdentityID: "user3",
			Type:       "password",
			IsActive:   false,
		},
	}

	assert.Len(t, allCredentials, 5, "Expected 5 credentials (Alice's, Bob's, Charlie's), got %d", len(allCredentials))

	for _, credential := range allCredentials {
		// Verify deleted credentials are not in the list
		assert.NotEqual(t, "cred_deleted_1", credential.ID, "cred_deleted_1 should not exist (was soft deleted from start)")
		assert.NotEqual(t, "cred_deleted_2", credential.ID, "cred_deleted_2 should not exist (was soft deleted from start)")

		// Verify all fields for all credentials
		expected, exists := expectedCredentials[credential.ID]
		require.True(t, exists, "Unexpected credential ID: %s", credential.ID)
		assert.Equal(t, expected.IdentityID, credential.IdentityID, "IdentityID mismatch for credential %s", credential.ID)
		assert.Equal(t, expected.Type, credential.Type, "Type mismatch for credential %s", credential.ID)
		assert.Equal(t, expected.IsActive, credential.IsActive, "IsActive mismatch for credential %s", credential.ID)
		assert.False(t, credential.CreatedAt.IsZero(), "CreatedAt should not be zero for credential %s", credential.ID)
		assert.False(t, credential.UpdatedAt.IsZero(), "UpdatedAt should not be zero for credential %s", credential.ID)
		t.Logf("   - Verified credential %s (%s): type=%s, active=%v",
			credential.ID, credential.IdentityID, credential.Type, credential.IsActive)
	}

	// Query all credential identifiers with all fields
	identifiersQuery := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.credential_identifiers` ORDER BY id", projectID, datasetID))
	identifiersIt, err := identifiersQuery.Read(ctx)
	require.NoError(t, err, "Failed to query credential identifiers from BigQuery")

	var allIdentifiers []BQCredentialIdentifier
	for {
		var identifier BQCredentialIdentifier
		err := identifiersIt.Next(&identifier)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err)
		allIdentifiers = append(allIdentifiers, identifier)
	}

	// Verify all credential identifiers: should have identifiers for cred1, cred2, cred3, cred4, cred5
	// cred_deleted_1_identifier and cred_deleted_2_identifier should NOT exist
	expectedIdentifiers := map[string]BQCredentialIdentifier{
		"cred1_identifier": {
			ID:           "cred1_identifier",
			CredentialID: "cred1",
			Type:         "email",
			Value:        "alice@example.com",
		},
		"cred2_identifier": {
			ID:           "cred2_identifier",
			CredentialID: "cred2",
			Type:         "oauth_provider",
			Value:        "google",
		},
		"cred3_identifier": {
			ID:           "cred3_identifier",
			CredentialID: "cred3",
			Type:         "email",
			Value:        "bob@example.com",
		},
		"cred4_identifier": {
			ID:           "cred4_identifier",
			CredentialID: "cred4",
			Type:         "api_key_name",
			Value:        "bob_api_key",
		},
		"cred5_identifier": {
			ID:           "cred5_identifier",
			CredentialID: "cred5",
			Type:         "email",
			Value:        "charlie@example.com",
		},
	}

	assert.Len(t, allIdentifiers, 5, "Expected 5 credential identifiers, got %d", len(allIdentifiers))

	emailIdentifierCount := 0
	oauthIdentifierCount := 0
	apiKeyIdentifierCount := 0

	for _, identifier := range allIdentifiers {
		// Verify deleted identifiers are not in the list
		assert.NotEqual(t, "cred_deleted_1_identifier", identifier.ID, "cred_deleted_1_identifier should not exist (was soft deleted from start)")
		assert.NotEqual(t, "cred_deleted_2_identifier", identifier.ID, "cred_deleted_2_identifier should not exist (was soft deleted from start)")

		// Verify all fields for all identifiers
		expected, exists := expectedIdentifiers[identifier.ID]
		require.True(t, exists, "Unexpected identifier ID: %s", identifier.ID)
		assert.Equal(t, expected.CredentialID, identifier.CredentialID, "CredentialID mismatch for identifier %s", identifier.ID)
		assert.Equal(t, expected.Type, identifier.Type, "Type mismatch for identifier %s", identifier.ID)
		assert.Equal(t, expected.Value, identifier.Value, "Value mismatch for identifier %s", identifier.ID)
		assert.False(t, identifier.CreatedAt.IsZero(), "CreatedAt should not be zero for identifier %s", identifier.ID)
		assert.False(t, identifier.UpdatedAt.IsZero(), "UpdatedAt should not be zero for identifier %s", identifier.ID)

		switch identifier.Type {
		case "email":
			emailIdentifierCount++
		case "oauth_provider":
			oauthIdentifierCount++
		case "api_key_name":
			apiKeyIdentifierCount++
		}

		t.Logf("   - Verified identifier %s (cred:%s): type=%s, value=%s",
			identifier.ID, identifier.CredentialID, identifier.Type, identifier.Value)
	}

	// Verify identifier type counts
	assert.Equal(t, 3, emailIdentifierCount, "Expected 3 email identifiers (password credentials for Alice, Bob, Charlie)")
	assert.Equal(t, 1, oauthIdentifierCount, "Expected 1 oauth_provider identifier (OAuth credential for Alice)")
	assert.Equal(t, 1, apiKeyIdentifierCount, "Expected 1 api_key_name identifier (API key credential for Bob)")

	t.Log("✅ BigQuery target data verification completed successfully")
	t.Logf("   - Identities: %d records (Alice, Bob, Charlie)", len(allIdentities))
	t.Logf("   - Credentials: %d records (Alice's, Bob's, Charlie's)", len(allCredentials))
	t.Logf("   - Credential Identifiers: %d records", len(allIdentifiers))
	t.Log("   Verification details:")
	t.Logf("     - Email identifiers: %d", emailIdentifierCount)
	t.Logf("     - OAuth identifiers: %d", oauthIdentifierCount)
	t.Logf("     - API key identifiers: %d", apiKeyIdentifierCount)
}

// verifyBigQueryTargetDataAfterDeletion verifies that Alice and her credentials are physically deleted from BigQuery
// It verifies all records and all fields to ensure data integrity
func verifyBigQueryTargetDataAfterDeletion(t *testing.T, ctx context.Context, client *bigquery.Client, dataset *bigquery.Dataset) {
	projectID := client.Project()
	datasetID := dataset.DatasetID

	// Query all identities with all fields to verify Alice (user1) is physically deleted
	identitiesQuery := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.identities` ORDER BY id", projectID, datasetID))
	identitiesIt, err := identitiesQuery.Read(ctx)
	require.NoError(t, err, "Failed to query identities from BigQuery")

	var allIdentities []BQIdentity
	for {
		var identity BQIdentity
		err := identitiesIt.Next(&identity)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err)
		allIdentities = append(allIdentities, identity)
	}

	// Verify all identities: should only have Bob (user2) and Charlie (user3)
	expectedIdentities := map[string]BQIdentity{
		"user2": {
			ID:          "user2",
			Username:    "bob",
			Email:       "bob@example.com",
			DisplayName: "Bob Smith",
			Status:      "active",
		},
		"user3": {
			ID:          "user3",
			Username:    "charlie",
			Email:       "charlie@example.com",
			DisplayName: "Charlie Wilson",
			Status:      "inactive",
		},
	}

	assert.Len(t, allIdentities, 2, "Expected 2 identities (Bob and Charlie), got %d", len(allIdentities))

	for _, identity := range allIdentities {
		// Verify deleted users are not in the list
		assert.NotEqual(t, "user1", identity.ID, "Alice (user1) should be physically deleted from identities")
		assert.NotEqual(t, "user_deleted_1", identity.ID, "user_deleted_1 should not exist (was soft deleted from start)")
		assert.NotEqual(t, "user_deleted_2", identity.ID, "user_deleted_2 should not exist (was soft deleted from start)")

		// Verify all fields for remaining identities
		expected, exists := expectedIdentities[identity.ID]
		require.True(t, exists, "Unexpected identity ID: %s", identity.ID)
		assert.Equal(t, expected.Username, identity.Username, "Username mismatch for identity %s", identity.ID)
		assert.Equal(t, expected.Email, identity.Email, "Email mismatch for identity %s", identity.ID)
		assert.Equal(t, expected.DisplayName, identity.DisplayName, "DisplayName mismatch for identity %s", identity.ID)
		assert.Equal(t, expected.Status, identity.Status, "Status mismatch for identity %s", identity.ID)
		assert.False(t, identity.CreatedAt.IsZero(), "CreatedAt should not be zero for identity %s", identity.ID)
		assert.False(t, identity.UpdatedAt.IsZero(), "UpdatedAt should not be zero for identity %s", identity.ID)
		t.Logf("   - Verified identity %s (%s): %s <%s> [%s]",
			identity.ID, identity.Username, identity.DisplayName, identity.Email, identity.Status)
	}

	// Query all credentials with all fields to verify Alice's credentials are physically deleted
	credentialsQuery := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.credentials` ORDER BY id", projectID, datasetID))
	credentialsIt, err := credentialsQuery.Read(ctx)
	require.NoError(t, err, "Failed to query credentials from BigQuery")

	var allCredentials []BQCredential
	for {
		var credential BQCredential
		err := credentialsIt.Next(&credential)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err)
		allCredentials = append(allCredentials, credential)
	}

	// Verify all credentials: should only have Bob's and Charlie's credentials
	expectedCredentials := map[string]BQCredential{
		"cred3": {
			ID:         "cred3",
			IdentityID: "user2",
			Type:       "password",
			IsActive:   true,
		},
		"cred4": {
			ID:         "cred4",
			IdentityID: "user2",
			Type:       "api_key",
			IsActive:   true,
		},
		"cred5": {
			ID:         "cred5",
			IdentityID: "user3",
			Type:       "password",
			IsActive:   false,
		},
	}

	assert.Len(t, allCredentials, 3, "Expected 3 credentials (Bob and Charlie's), got %d", len(allCredentials))

	for _, credential := range allCredentials {
		// Verify deleted credentials are not in the list
		assert.NotEqual(t, "cred1", credential.ID, "Alice's credential (cred1) should be physically deleted")
		assert.NotEqual(t, "cred2", credential.ID, "Alice's credential (cred2) should be physically deleted")
		assert.NotEqual(t, "cred_deleted_1", credential.ID, "cred_deleted_1 should not exist (was soft deleted from start)")
		assert.NotEqual(t, "cred_deleted_2", credential.ID, "cred_deleted_2 should not exist (was soft deleted from start)")

		// Verify all fields for remaining credentials
		expected, exists := expectedCredentials[credential.ID]
		require.True(t, exists, "Unexpected credential ID: %s", credential.ID)
		assert.Equal(t, expected.IdentityID, credential.IdentityID, "IdentityID mismatch for credential %s", credential.ID)
		assert.Equal(t, expected.Type, credential.Type, "Type mismatch for credential %s", credential.ID)
		assert.Equal(t, expected.IsActive, credential.IsActive, "IsActive mismatch for credential %s", credential.ID)
		assert.False(t, credential.CreatedAt.IsZero(), "CreatedAt should not be zero for credential %s", credential.ID)
		assert.False(t, credential.UpdatedAt.IsZero(), "UpdatedAt should not be zero for credential %s", credential.ID)
		t.Logf("   - Verified credential %s (%s): type=%s, active=%v",
			credential.ID, credential.IdentityID, credential.Type, credential.IsActive)
	}

	// Query all credential identifiers with all fields to verify Alice's identifiers are physically deleted
	identifiersQuery := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.credential_identifiers` ORDER BY id", projectID, datasetID))
	identifiersIt, err := identifiersQuery.Read(ctx)
	require.NoError(t, err, "Failed to query credential identifiers from BigQuery")

	var allIdentifiers []BQCredentialIdentifier
	for {
		var identifier BQCredentialIdentifier
		err := identifiersIt.Next(&identifier)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err)
		allIdentifiers = append(allIdentifiers, identifier)
	}

	// Verify all credential identifiers: should only have Bob's and Charlie's identifiers
	expectedIdentifiers := map[string]BQCredentialIdentifier{
		"cred3_identifier": {
			ID:           "cred3_identifier",
			CredentialID: "cred3",
			Type:         "email",
			Value:        "bob@example.com",
		},
		"cred4_identifier": {
			ID:           "cred4_identifier",
			CredentialID: "cred4",
			Type:         "api_key_name",
			Value:        "bob_api_key",
		},
		"cred5_identifier": {
			ID:           "cred5_identifier",
			CredentialID: "cred5",
			Type:         "email",
			Value:        "charlie@example.com",
		},
	}

	assert.Len(t, allIdentifiers, 3, "Expected 3 credential identifiers (Bob and Charlie's), got %d", len(allIdentifiers))

	for _, identifier := range allIdentifiers {
		// Verify deleted identifiers are not in the list
		assert.NotEqual(t, "cred1_identifier", identifier.ID, "Alice's credential identifier (cred1_identifier) should be physically deleted")
		assert.NotEqual(t, "cred2_identifier", identifier.ID, "Alice's credential identifier (cred2_identifier) should be physically deleted")
		assert.NotEqual(t, "cred_deleted_1_identifier", identifier.ID, "cred_deleted_1_identifier should not exist (was soft deleted from start)")
		assert.NotEqual(t, "cred_deleted_2_identifier", identifier.ID, "cred_deleted_2_identifier should not exist (was soft deleted from start)")

		// Verify all fields for remaining identifiers
		expected, exists := expectedIdentifiers[identifier.ID]
		require.True(t, exists, "Unexpected identifier ID: %s", identifier.ID)
		assert.Equal(t, expected.CredentialID, identifier.CredentialID, "CredentialID mismatch for identifier %s", identifier.ID)
		assert.Equal(t, expected.Type, identifier.Type, "Type mismatch for identifier %s", identifier.ID)
		assert.Equal(t, expected.Value, identifier.Value, "Value mismatch for identifier %s", identifier.ID)
		assert.False(t, identifier.CreatedAt.IsZero(), "CreatedAt should not be zero for identifier %s", identifier.ID)
		assert.False(t, identifier.UpdatedAt.IsZero(), "UpdatedAt should not be zero for identifier %s", identifier.ID)
		t.Logf("   - Verified identifier %s (cred:%s): type=%s, value=%s",
			identifier.ID, identifier.CredentialID, identifier.Type, identifier.Value)
	}

	t.Log("✅ Verified: Alice and all her related data have been physically deleted from BigQuery")
	t.Logf("   - Identities: %d records (Bob and Charlie)", len(allIdentities))
	t.Logf("   - Credentials: %d records (Bob's and Charlie's)", len(allCredentials))
	t.Logf("   - Credential Identifiers: %d records (Bob's and Charlie's)", len(allIdentifiers))
}

// setupTestDatabasesForBQ sets up source database and pipeline database for BigQuery target tests
func setupTestDatabasesForBQ(t *testing.T, ctx context.Context) (*gorm.DB, *sql.DB) {
	// Setup source database
	sourceSuite := gormx.MustStartTestSuite(ctx)
	t.Cleanup(func() { _ = sourceSuite.Stop(context.Background()) })
	t.Logf("SourceDB: %s", sourceSuite.DSN())

	sourceDB := sourceSuite.DB()
	require.NoError(t, sourceDB.AutoMigrate(&User{}, &UserCred{}), "Failed to migrate source database")

	// Setup pipeline database
	pipelineSuite := gormx.MustStartTestSuite(ctx)
	t.Cleanup(func() { _ = pipelineSuite.Stop(context.Background()) })
	t.Logf("PipelineDB: %s", pipelineSuite.DSN())

	pipelineSQLDB, err := pipelineSuite.DB().DB()
	require.NoError(t, err, "Failed to get sql.DB from pipeline database")
	require.NoError(t, pg.Migrate(pipelineSQLDB), "Failed to migrate pipeline database")

	return sourceDB, pipelineSQLDB
}

// setupBQTables creates or ensures the BigQuery dataset and tables exist
func setupBQTables(t *testing.T, ctx context.Context, projectID, datasetID string) (*bigquery.Client, *bigquery.Dataset) {
	client, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create BigQuery client")
	t.Cleanup(func() { _ = client.Close() })

	// Create dataset if it doesn't exist
	dataset := client.Dataset(datasetID)
	meta, err := dataset.Metadata(ctx)
	if err != nil {
		if bqtarget.IsNotFound(err) {
			// Dataset doesn't exist, create it
			meta := &bigquery.DatasetMetadata{
				Location: "asia-northeast1", // Tokyo region
			}
			err = dataset.Create(ctx, meta)
			require.NoError(t, err, "Failed to create dataset")
			t.Logf("Created dataset: %s.%s (location: asia-northeast1, Tokyo)", projectID, datasetID)
		} else {
			require.NoError(t, err, "Failed to check dataset existence %s", datasetID)
		}
	} else {
		// Dataset exists, use it
		t.Logf("Using existing dataset: %s.%s (location: %s)", projectID, datasetID, meta.Location)
	}

	// Create tables in the dataset
	createTables(t, ctx, client, dataset)

	return client, dataset
}

// TableConfig represents a table configuration
type TableConfig struct {
	Name  string
	Model interface{}
}

func createTables(t *testing.T, ctx context.Context, client *bigquery.Client, dataset *bigquery.Dataset) {
	tableConfigs := []TableConfig{
		{
			Name:  "identities",
			Model: BQIdentity{},
		},
		{
			Name:  "credentials",
			Model: BQCredential{},
		},
		{
			Name:  "credential_identifiers",
			Model: BQCredentialIdentifier{},
		},
	}

	for _, config := range tableConfigs {
		recreateTable(t, ctx, client, dataset, config)
	}
}

// recreateTable creates the table if it doesn't exist, or truncates it if it exists
// This ensures a clean state for each test run
func recreateTable(t *testing.T, ctx context.Context, client *bigquery.Client, dataset *bigquery.Dataset, config TableConfig) {
	table := dataset.Table(config.Name)

	// Infer schema
	schema, err := bigquery.InferSchema(config.Model)
	require.NoError(t, err, "Failed to infer schema for %s", config.Name)

	// Remove Metadata field from schema (target tables don't need it, only staging tables do)
	schema = removeMetadataField(schema)

	// Always set primary key first
	setPrimaryKey(schema)

	meta := &bigquery.TableMetadata{
		Schema: schema,
	}

	// Try to create the table
	err = table.Create(ctx, meta)
	if err != nil {
		// If table already exists, truncate it using TruncateTableIfExists
		err = bqtarget.TruncateTableIfExists(ctx, err, client, dataset.DatasetID, config.Name)
		require.NoError(t, err, "Failed to create or truncate table %s", config.Name)
		t.Logf("Truncated existing table: %s", config.Name)
	} else {
		t.Logf("Created table: %s", config.Name)
	}
}

// removeMetadataField removes the __etl_metadata__ field from schema
// Target tables don't need this field, only staging tables do
func removeMetadataField(schema bigquery.Schema) bigquery.Schema {
	var filteredSchema bigquery.Schema
	for _, field := range schema {
		if field.Name != "__etl_metadata__" {
			filteredSchema = append(filteredSchema, field)
		}
	}
	return filteredSchema
}

// setPrimaryKey ensures the id field is set as primary key (Required: true)
func setPrimaryKey(schema bigquery.Schema) {
	for i := range schema {
		if schema[i].Name == "id" {
			schema[i].Required = true
			schema[i].Description = "Primary key"
			break
		}
	}
}
