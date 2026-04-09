package etl_test

import (
	"context"
	"testing"
	"time"

	"github.com/theplant/etl"
	"github.com/theplant/etl/pgtarget"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// ====== Optimized Source Database Model ======
// The key difference from the original User model is the CredentialUpdatedAt column:
// a denormalized timestamp maintained by the application layer (or a DB trigger) whenever
// any related user_creds row is created, updated, or deleted.
//
// This eliminates the need for LEFT JOIN + GROUP BY on user_creds at query time.

type OptimizedUser struct {
	ID                  string `gorm:"primaryKey"`
	Username            string
	Email               string
	DisplayName         string
	Status              string
	CredentialUpdatedAt time.Time `gorm:"index"` // Denormalized: latest change time across all related user_creds. Maintained on write.
	CreatedAt           time.Time
	UpdatedAt           time.Time
	DeletedAt           gorm.DeletedAt
	UserCreds           []*UserCred `gorm:"foreignKey:UserID"` // reuse UserCred from pipeline_test.go
}

// ====== Optimized Source Implementation ======

type optimizedIdentitySyncer struct {
	sourceDB *gorm.DB
	targetDB *gorm.DB
}

var _ etl.Source[*etl.Cursor] = (*optimizedIdentitySyncer)(nil)

func (s *optimizedIdentitySyncer) Extract(ctx context.Context, req *etl.ExtractRequest[*etl.Cursor]) (*etl.ExtractResponse[*etl.Cursor], error) {
	cursor := req.After

	// Optimized approach: paginate by (max_at, id) on a single table.
	//
	// By denormalizing credential_updated_at onto the user record (maintained by the application
	// layer or a DB trigger on every creds INSERT/UPDATE/DELETE), updating any credential
	// necessarily writes to the user row, bumping its updated_at. This means we only need to
	// monitor the user table's updated_at to capture both user-level AND credential-level changes,
	// eliminating the expensive LEFT JOIN + GROUP BY + CTE required in pipeline_test.go's approach.
	//
	// max_at = GREATEST(updated_at, COALESCE(deleted_at, updated_at)) is needed because
	// vanilla GORM only sets deleted_at on soft delete without updating updated_at.
	// If you want to paginate solely on updated_at (without GREATEST), enable
	// gormx.SoftDeleteUpdatedAtPlugin — it updates updated_at alongside deleted_at
	// on soft delete, so updated_at alone becomes a reliable sync cursor.
	//
	// The result is a simple single-table query with no JOIN, GROUP BY, CTE, or subquery.
	//
	// Note: Query ALL records including soft-deleted ones for soft->physical delete conversion.

	usersQuery := `
		WITH user_max_at AS (
			SELECT *, GREATEST(updated_at, COALESCE(deleted_at, updated_at)) AS max_at
			FROM optimized_users
		)
		SELECT * FROM user_max_at uma
		WHERE 1=1`
	args := []any{}

	if cursor != nil && (!cursor.At.IsZero() || cursor.ID != "") {
		usersQuery += ` AND (uma.max_at, uma.id) > (?, ?)`
		args = append(args, cursor.At, cursor.ID)
	}

	usersQuery += ` AND uma.max_at >= ? AND uma.max_at < ?`
	args = append(args, req.FromAt, req.BeforeAt)

	usersQuery += ` ORDER BY uma.max_at ASC, uma.id ASC LIMIT ?`
	args = append(args, req.First+1)

	// Define a struct to capture the Raw query results including max_at
	type OptimizedUserWithMaxAt struct {
		*OptimizedUser
		MaxAt time.Time `gorm:"column:max_at"`
	}

	var users []*OptimizedUserWithMaxAt
	if err := s.sourceDB.WithContext(ctx).
		Unscoped().
		Raw(usersQuery, args...).
		Preload("UserCreds").
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

	// Create end cursor using max_at
	last := users[len(users)-1]
	endCursor := &etl.Cursor{
		At: last.MaxAt,
		ID: last.ID,
	}

	// Extract OptimizedUser slice for transform
	optimizedUsers := make([]*OptimizedUser, len(users))
	for i, u := range users {
		optimizedUsers[i] = u.OptimizedUser
	}

	// Transform to target tables (reuse the same transform/commit logic)
	datas, err := s.transform(ctx, optimizedUsers)
	if err != nil {
		return nil, err
	}

	target, err := pgtarget.New(&pgtarget.Config[*etl.Cursor]{
		DB:               s.targetDB,
		Req:              req,
		Datas:            datas,
		CommitFunc:       s.commit,
		UseUnloggedTable: false,
	})
	if err != nil {
		return nil, err
	}
	target = target.WithCreateStagingTableHook(pgtarget.AddMetadataColumnHook[*etl.Cursor])

	return &etl.ExtractResponse[*etl.Cursor]{
		Target:      target,
		EndCursor:   endCursor,
		HasNextPage: hasNextPage,
	}, nil
}

func (s *optimizedIdentitySyncer) transform(_ context.Context, users []*OptimizedUser) (etl.TargetDatas, error) {
	type metadata struct {
		DeletedAt gorm.DeletedAt `json:"deletedAt"`
	}

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

	for _, user := range users {
		identity := &Identity{
			ID:          user.ID,
			Username:    user.Username,
			Email:       user.Email,
			DisplayName: user.DisplayName,
			Status:      user.Status,
			CreatedAt:   user.CreatedAt,
			UpdatedAt:   user.UpdatedAt,
		}
		identities = append(identities, &identityWithMetadata{
			Identity: identity,
			Metadata: datatypes.NewJSONType(&metadata{DeletedAt: user.DeletedAt}),
		})

		for _, userCred := range user.UserCreds {
			credential := &Credential{
				ID:         userCred.ID,
				IdentityID: userCred.UserID,
				Type:       userCred.CredType,
				Value:      userCred.CredValue,
				IsActive:   userCred.IsActive,
				ExpiresAt:  userCred.ExpiresAt,
				CreatedAt:  userCred.CreatedAt,
				UpdatedAt:  userCred.UpdatedAt,
			}
			credentials = append(credentials, &credentialWithMetadata{
				Credential: credential,
				Metadata:   datatypes.NewJSONType(&metadata{DeletedAt: userCred.DeletedAt}),
			})

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

			credentialIdentifier := &CredentialIdentifier{
				ID:           userCred.ID + "_identifier",
				CredentialID: userCred.ID,
				Type:         identifierType,
				Value:        userCred.Identifier,
				CreatedAt:    userCred.CreatedAt,
				UpdatedAt:    userCred.UpdatedAt,
			}
			credentialIdentifiers = append(credentialIdentifiers, &credentialIdentifierWithMetadata{
				CredentialIdentifier: credentialIdentifier,
				Metadata:             datatypes.NewJSONType(&metadata{DeletedAt: userCred.DeletedAt}),
			})
		}
	}

	return etl.TargetDatas{
		{Table: "identities", Records: identities},
		{Table: "credentials", Records: credentials},
		{Table: "credential_identifiers", Records: credentialIdentifiers},
	}, nil
}

func (s *optimizedIdentitySyncer) commit(ctx context.Context, input *pgtarget.CommitInput[*etl.Cursor]) (*pgtarget.CommitOutput[*etl.Cursor], error) {
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

	if err := input.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Exec(query).Error
	}); err != nil {
		return nil, errors.Wrapf(err, "failed to execute commit query")
	}

	return &pgtarget.CommitOutput[*etl.Cursor]{}, nil
}

// ====== Test ======

func TestIdentitySyncer_Optimized(t *testing.T) {
	ctx := context.Background()

	// Setup test databases
	sourceDB, targetDB, pipelineSQLDB := setupTestDatabases(t, ctx)

	// Migrate the optimized users table
	require.NoError(t, sourceDB.AutoMigrate(&OptimizedUser{}), "Failed to migrate optimized_users table")

	// Prepare test data
	prepareOptimizedSourceTestData(t, sourceDB)

	// Create and start pipeline
	pipeline, err := etl.NewPipeline(&etl.PipelineConfig[*etl.Cursor]{
		Source: &optimizedIdentitySyncer{
			sourceDB: sourceDB,
			targetDB: targetDB,
		},
		QueueDB:                 pipelineSQLDB,
		QueueName:               "optimized_identity_etl",
		PageSize:                10,
		Interval:                3 * time.Second,
		ConsistencyDelay:        1 * time.Second,
		RetryPolicy:             bus.DefaultRetryPolicyFactory(),
		CircuitBreakerThreshold: 3,
		CircuitBreakerCooldown:  60 * time.Second,
	})
	require.NoError(t, err, "Failed to create pipeline")

	controller, err := pipeline.Start(ctx, &etl.Cursor{})
	require.NoError(t, err, "Failed to start pipeline")
	defer func() { _ = controller.Stop(ctx) }()

	// === Round 1: Initial sync ===
	time.Sleep(2 * time.Second)

	verifyOptimizedFirstSync(t, targetDB)
	t.Log("Round 1: Initial sync completed successfully")

	// === Round 2: Soft delete user -> physical delete in target ===
	t.Log("Round 2: Soft delete Alice and sync")

	// Soft delete user1 (Alice)
	result := sourceDB.Where("id = ?", "user1").Delete(&OptimizedUser{})
	require.NoError(t, result.Error)
	require.Equal(t, int64(1), result.RowsAffected)

	// Soft delete ALL of Alice's credentials
	result = sourceDB.Where("user_id = ?", "user1").Delete(&UserCred{})
	require.NoError(t, result.Error)
	require.GreaterOrEqual(t, result.RowsAffected, int64(1))

	// Update credential_updated_at to reflect the creds change.
	// In production this would be done by the application layer or a DB trigger.
	// Note: updated_at is already bumped by the soft delete above (via gormx.SoftDeleteUpdatedAtPlugin),
	// and the Extract query uses GREATEST(updated_at, deleted_at) so it would capture this even
	// without the plugin. We only need to maintain credential_updated_at here.
	require.NoError(t, sourceDB.Unscoped().Model(&OptimizedUser{}).Where("id = ?", "user1").
		Update("credential_updated_at", time.Now()).Error)

	time.Sleep(5 * time.Second)

	verifyOptimizedAfterDeletion(t, targetDB)
	t.Log("Round 2: Soft delete -> physical delete completed successfully")

	// === Round 3: Update credential -> incremental sync ===
	t.Log("Round 3: Update Bob's credential and sync")

	// Update Bob's password credential value
	now := time.Now()
	require.NoError(t, sourceDB.Model(&UserCred{}).Where("id = ?", "cred3").
		Updates(map[string]any{
			"cred_value": "new_hashed_password_bob",
			"updated_at": now,
		}).Error)

	// Update Bob's credential_updated_at and updated_at to reflect the creds change.
	// In production this would be done by the application layer or a DB trigger.
	require.NoError(t, sourceDB.Model(&OptimizedUser{}).Where("id = ?", "user2").
		Updates(map[string]any{
			"credential_updated_at": now,
			"updated_at":            now,
		}).Error)

	time.Sleep(5 * time.Second)

	verifyOptimizedAfterCredUpdate(t, targetDB)
	t.Log("Round 3: Credential update sync completed successfully")
}

// ====== Verification Functions ======

// verifyOptimizedFirstSync verifies the target after initial sync:
// - 3 active users synced (Alice, Bob, Charlie)
// - 5 active credentials synced
// - Soft-deleted users (Dave, Eve) NOT synced
// - Soft-deleted credentials (cred_deleted_1, cred_deleted_2) NOT synced
func verifyOptimizedFirstSync(t *testing.T, db *gorm.DB) {
	t.Helper()

	// Verify identities: only active users
	var identities []Identity
	require.NoError(t, db.Find(&identities).Error)
	assert.Len(t, identities, 3, "should have 3 identities (Alice, Bob, Charlie)")

	identityMap := make(map[string]Identity)
	for _, id := range identities {
		identityMap[id.ID] = id
	}

	alice, exists := identityMap["user1"]
	require.True(t, exists, "Alice should exist")
	assert.Equal(t, "alice", alice.Username)
	assert.Equal(t, "alice@example.com", alice.Email)

	// Negative: soft-deleted users should NOT be in target
	for _, deletedID := range []string{"user_deleted_1", "user_deleted_2"} {
		_, exists := identityMap[deletedID]
		assert.False(t, exists, "%s should NOT exist in target (soft-deleted from start)", deletedID)
	}

	// Verify credentials: only active ones
	var credentials []Credential
	require.NoError(t, db.Find(&credentials).Error)
	assert.Len(t, credentials, 5, "should have 5 credentials (2 Alice, 2 Bob, 1 Charlie)")

	credMap := make(map[string]Credential)
	for _, c := range credentials {
		credMap[c.ID] = c
	}

	// Negative: soft-deleted credentials should NOT be in target
	for _, deletedID := range []string{"cred_deleted_1", "cred_deleted_2"} {
		_, exists := credMap[deletedID]
		assert.False(t, exists, "%s should NOT exist in target (soft-deleted from start)", deletedID)
	}

	// Verify credential identifiers
	var credIdentifiers []CredentialIdentifier
	require.NoError(t, db.Find(&credIdentifiers).Error)
	assert.Len(t, credIdentifiers, 5, "should have 5 credential identifiers")
}

// verifyOptimizedAfterDeletion verifies:
// - Alice and all her data are physically deleted from target
// - Bob and Charlie remain intact
func verifyOptimizedAfterDeletion(t *testing.T, db *gorm.DB) {
	t.Helper()

	// Alice should be physically deleted
	var identity Identity
	err := db.Where("id = ?", "user1").First(&identity).Error
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound, "Alice should be physically deleted from identities")

	// Alice's credentials should be physically deleted
	var aliceCreds []Credential
	require.NoError(t, db.Where("identity_id = ?", "user1").Find(&aliceCreds).Error)
	assert.Empty(t, aliceCreds, "Alice's credentials should be physically deleted")

	// Alice's credential identifiers should be physically deleted
	var aliceIdentifiers []CredentialIdentifier
	require.NoError(t, db.Where("id IN ?", []string{"cred1_identifier", "cred2_identifier"}).Find(&aliceIdentifiers).Error)
	assert.Empty(t, aliceIdentifiers, "Alice's credential identifiers should be physically deleted")

	// Bob and Charlie should still exist
	var remaining []Identity
	require.NoError(t, db.Find(&remaining).Error)
	assert.Len(t, remaining, 2, "Bob and Charlie should remain")

	remainingMap := make(map[string]Identity)
	for _, id := range remaining {
		remainingMap[id.ID] = id
	}
	assert.Contains(t, remainingMap, "user2", "Bob should still exist")
	assert.Contains(t, remainingMap, "user3", "Charlie should still exist")

	// Bob's credentials should be intact
	var bobCreds []Credential
	require.NoError(t, db.Where("identity_id = ?", "user2").Find(&bobCreds).Error)
	assert.Len(t, bobCreds, 2, "Bob should still have 2 credentials")

	// Charlie's credentials should be intact
	var charlieCreds []Credential
	require.NoError(t, db.Where("identity_id = ?", "user3").Find(&charlieCreds).Error)
	assert.Len(t, charlieCreds, 1, "Charlie should still have 1 credential")
}

// verifyOptimizedAfterCredUpdate verifies that the incremental credential update
// is correctly synced to the target (Bob's password changed).
func verifyOptimizedAfterCredUpdate(t *testing.T, db *gorm.DB) {
	t.Helper()

	// Bob's password credential should have the updated value
	var cred Credential
	require.NoError(t, db.Where("id = ?", "cred3").First(&cred).Error)
	assert.Equal(t, "new_hashed_password_bob", cred.Value, "Bob's password should be updated")

	// Overall counts should not change from round 2
	var identities []Identity
	require.NoError(t, db.Find(&identities).Error)
	assert.Len(t, identities, 2, "should still have 2 identities (Bob, Charlie)")

	var credentials []Credential
	require.NoError(t, db.Find(&credentials).Error)
	assert.Len(t, credentials, 3, "should still have 3 credentials (2 Bob, 1 Charlie)")

	var credIdentifiers []CredentialIdentifier
	require.NoError(t, db.Find(&credIdentifiers).Error)
	assert.Len(t, credIdentifiers, 3, "should still have 3 credential identifiers")
}

// ====== Test Data ======

func prepareOptimizedSourceTestData(t *testing.T, db *gorm.DB) {
	now := time.Now()

	users := []OptimizedUser{
		{
			ID:                  "user1",
			Username:            "alice",
			Email:               "alice@example.com",
			DisplayName:         "Alice Johnson",
			Status:              "active",
			CredentialUpdatedAt: now.AddDate(0, 0, -1),
			CreatedAt:           now.AddDate(0, 0, -2),
			UpdatedAt:           now.AddDate(0, 0, -1),
		},
		{
			ID:                  "user2",
			Username:            "bob",
			Email:               "bob@example.com",
			DisplayName:         "Bob Smith",
			Status:              "active",
			CredentialUpdatedAt: now.AddDate(0, 0, -1),
			CreatedAt:           now.AddDate(0, 0, -2),
			UpdatedAt:           now.AddDate(0, 0, -1),
		},
		{
			ID:                  "user3",
			Username:            "charlie",
			Email:               "charlie@example.com",
			DisplayName:         "Charlie Wilson",
			Status:              "inactive",
			CredentialUpdatedAt: now.AddDate(0, 0, -1),
			CreatedAt:           now.AddDate(0, 0, -2),
			UpdatedAt:           now.AddDate(0, 0, -1),
		},
	}
	require.NoError(t, db.Create(&users).Error, "Failed to create users")

	expiresAt := now.AddDate(1, 0, 0)
	userCreds := []UserCred{
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
		{
			ID:         "cred5",
			UserID:     "user3",
			CredType:   "password",
			Identifier: "charlie@example.com",
			CredValue:  "hashed_password_charlie",
			IsActive:   false,
			CreatedAt:  now.AddDate(0, 0, -2),
			UpdatedAt:  now.AddDate(0, 0, -1),
		},
	}
	require.NoError(t, db.Create(&userCreds).Error, "Failed to create user credentials")

	// Soft-deleted data
	deletedAt := now.AddDate(0, 0, -5)
	deletedUsers := []OptimizedUser{
		{
			ID:                  "user_deleted_1",
			Username:            "deleted_dave",
			Email:               "dave@example.com",
			DisplayName:         "Dave (Deleted)",
			Status:              "inactive",
			CredentialUpdatedAt: now.AddDate(0, 0, -6),
			CreatedAt:           now.AddDate(0, 0, -10),
			UpdatedAt:           now.AddDate(0, 0, -6),
			DeletedAt:           gorm.DeletedAt{Time: deletedAt, Valid: true},
		},
		{
			ID:                  "user_deleted_2",
			Username:            "deleted_eve",
			Email:               "eve@example.com",
			DisplayName:         "Eve (Deleted)",
			Status:              "inactive",
			CredentialUpdatedAt: deletedAt,
			CreatedAt:           now.AddDate(0, 0, -8),
			UpdatedAt:           now.AddDate(0, 0, -4),
			DeletedAt:           gorm.DeletedAt{Time: deletedAt, Valid: true},
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
			DeletedAt:  gorm.DeletedAt{Time: deletedAt, Valid: true},
		},
		{
			ID:         "cred_deleted_2",
			UserID:     "user1",
			CredType:   "old_password",
			Identifier: "alice@example.com",
			CredValue:  "old_hashed_password_alice",
			IsActive:   false,
			CreatedAt:  now.AddDate(0, 0, -15),
			UpdatedAt:  now.AddDate(0, 0, -10),
			DeletedAt:  gorm.DeletedAt{Time: deletedAt, Valid: true},
		},
	}
	require.NoError(t, db.Create(&deletedUserCreds).Error, "Failed to create deleted user credentials")

	t.Log("Optimized source test data prepared successfully")
	t.Logf("   - Users: %d (+ %d soft deleted)", len(users), len(deletedUsers))
	t.Logf("   - UserCreds: %d (+ %d soft deleted)", len(userCreds), len(deletedUserCreds))
}
