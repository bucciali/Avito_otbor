package dbtablesgo

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/lib/pq"
)

const (
	Format = "20060102"
)

var (
	Db *sql.DB
)

type TeamMember struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	IsActive bool   `json:"is_active"`
}

type Team struct {
	TeamName string       `json:"team_name"`
	Members  []TeamMember `json:"members"`
}

type User struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	TeamName string `json:"team_name"`
	IsActive bool   `json:"is_active"`
}

type PullRequest struct {
	PullRequestID     string     `json:"pull_request_id"`
	PullRequestName   string     `json:"pull_request_name"`
	AuthorID          string     `json:"author_id"`
	Status            string     `json:"status"`
	AssignedReviewers []string   `json:"assigned_reviewers"`
	CreatedAt         time.Time  `json:"createdAt,omitempty"`
	MergedAt          *time.Time `json:"mergedAt,omitempty"`
}

type Stats struct {
	AssignmentsByUser map[string]int `json:"assignments_by_user"`
	AssignmentsByPR   map[string]int `json:"assignments_by_pr"`
}

func GetConnection() string {
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASS")
	name := os.Getenv("DB_NAME")

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, pass, host, port, name,
	)
}

func DbInit() error {
	conn := GetConnection()
	var err error
	Db, err = sql.Open("postgres", conn)
	if err != nil {
		return err
	}
	err = Db.Ping()
	if err != nil {
		return err
	}
	err = CreateTables()
	return err

}
func selectReviewer(tx *sql.Tx, authorID string) (string, error) {
	var teamID string
	err := tx.QueryRow(`SELECT team_id FROM users WHERE id = $1`, authorID).Scan(&teamID)
	if err != nil {
		return "", err
	}

	rows, err := tx.Query(`
        SELECT id 
        FROM users 
        WHERE team_id = $1 AND is_active = true AND id != $2
    `, teamID, authorID)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var reviewers []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", fmt.Errorf("scan id: %w", err)
		}
		reviewers = append(reviewers, id)
	}

	if len(reviewers) == 0 {
		return "", errors.New("NO_REVIEWERS")
	}
	rows2, err := tx.Query(`
        SELECT reviewer_id, COUNT(*) as cnt
        FROM pull_requests
        WHERE reviewer_id = ANY($1)
        GROUP BY reviewer_id
        ORDER BY cnt ASC
        LIMIT 1
    `, pq.Array(reviewers))
	if err != nil {
		return "", err
	}
	defer rows2.Close()

	if rows2.Next() {
		var reviewerID string
		var cnt int
		if err := rows2.Scan(&reviewerID, &cnt); err != nil {
			return "", fmt.Errorf("scan reviewer stats: %w", err)
		}
		return reviewerID, nil
	}
	return reviewers[0], nil
}

func contains(arr []string, target string) bool {
	for _, v := range arr {
		if v == target {
			return true
		}
	}
	return false
}

func DeactivateManyUsers(ids []string) error {
	tx, err := Db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(`UPDATE users SET is_active = false WHERE id = ANY($1)`, pq.Array(ids))
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}
		return err
	}
	rows, err := tx.Query(`
        SELECT id, author_id
        FROM pull_requests
        WHERE reviewer_id = ANY($1) AND status = 'OPEN'
    `, pq.Array(ids))
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return err
		}
		return err
	}

	prs := []struct{ id, author string }{}
	for rows.Next() {
		var id, author string
		if err := rows.Scan(&id, &author); err != nil {
			return fmt.Errorf("scan pr data: %w", err)
		}
		prs = append(prs, struct{ id, author string }{id, author})
	}

	for _, pr := range prs {
		reviewer, err := selectReviewer(tx, pr.author)
		if err != nil {
			if err := tx.Rollback(); err != nil {
				return err
			}
			return errors.New("NO_REVIEWERS")
		}

		_, err = tx.Exec(`
            UPDATE pull_requests SET reviewer_id = $1 WHERE id = $2
        `, reviewer, pr.id)
		if err != nil {
			if err := tx.Rollback(); err != nil {
				return err
			}
			return err
		}
	}

	return tx.Commit()
}

func GetStats() (*Stats, error) {
	stats := &Stats{
		AssignmentsByUser: make(map[string]int),
		AssignmentsByPR:   make(map[string]int),
	}

	rows, err := Db.Query(`
        SELECT reviewer_id, COUNT(*) 
        FROM pull_requests 
        GROUP BY reviewer_id
    `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var user string
		var count int
		if err := rows.Scan(&user, &count); err != nil {
			return nil, fmt.Errorf("scan user stats: %w", err)
		}
		stats.AssignmentsByUser[user] = count
	}
	rows2, err := Db.Query(`
        SELECT id, 1 
        FROM pull_requests
    `)
	if err != nil {
		return nil, err
	}
	defer rows2.Close()

	for rows2.Next() {
		var pr string
		var one int
		if err := rows2.Scan(&pr, &one); err != nil {
			return nil, fmt.Errorf("scan pr stats: %w", err)
		}
		stats.AssignmentsByPR[pr] = one
	}

	return stats, nil
}

func GetPR(prID string) (*PullRequest, error) {
	var pr PullRequest
	err := Db.QueryRow(`SELECT pr_id,
        pr_name,
        author_id,
        status,
		assigned_reviewers,
		created_at,
		merged_at
        FROM pull_requests where pr_id = $1`, prID).Scan(&pr.PullRequestID, &pr.PullRequestName, &pr.AuthorID, &pr.Status,
		pq.Array(&pr.AssignedReviewers), &pr.CreatedAt, &pr.MergedAt)

	if err != nil {
		return nil, err
	}
	return &pr, nil
}

func GetReview(userID string) ([]PullRequest, error) {
	rows, err := Db.Query(`SELECT pr_id,
        pr_name,
        author_id,
        status,
		assigned_reviewers,
		created_at,
		merged_at
        FROM pull_requests
        WHERE $1 = ANY(assigned_reviewers)`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []PullRequest{}
	for rows.Next() {
		var pr PullRequest
		err := rows.Scan(&pr.PullRequestID, &pr.PullRequestName, &pr.AuthorID, &pr.Status,
			pq.Array(&pr.AssignedReviewers), &pr.CreatedAt, &pr.MergedAt)
		if err != nil {
			return nil, err

		}
		result = append(result, pr)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, errors.New("NOT_FOUND")
	}

	return result, nil

}

func ChangeReviewer(prID, oldReviewerID string) (*PullRequest, string, error) {
	var pr PullRequest
	err := Db.QueryRow(`select pr_id,
    pr_name,
    author_id,
    status,
    assigned_reviewers,
    created_at,
    merged_at FROM pull_requests WHERE pr_id = $1`, prID).Scan(&pr.PullRequestID,
		&pr.PullRequestName, &pr.AuthorID, &pr.Status, &pr.AssignedReviewers, &pr.CreatedAt, &pr.MergedAt)
	if err == sql.ErrNoRows {
		return nil, "", errors.New("NOT_FOUND")
	}
	if err != nil {
		return nil, "", err
	}
	if pr.Status == "MERGED" {
		return nil, "", errors.New("MERGED_LOCKED")
	}
	if !contains(pr.AssignedReviewers, oldReviewerID) {
		return nil, "", errors.New("NOT_ASSIGNED")
	}
	var teamPrName string

	err = Db.QueryRow(`select team_name from users where user_id = $1`, pr.AuthorID).Scan(&teamPrName)
	if err != nil {
		return nil, "", err
	}
	var newID string
	err = Db.QueryRow(`
		select user_id
		from users 
		WHERE team_name = $1 and is_active = true and user_id != $2 and user_id != $3 ORDER BY RANDOM() limit 1 `,
		teamPrName, pr.AuthorID, oldReviewerID).Scan(&newID)
	if err == sql.ErrNoRows {
		return nil, "", errors.New("NO_REPLACEMENT_FOUND")
	}
	if err != nil {
		return nil, "", err
	}
	for k, v := range pr.AssignedReviewers {
		if v == oldReviewerID {
			pr.AssignedReviewers[k] = newID
		}
	}
	_, err = Db.Exec(`
        UPDATE pull_requests
        SET assigned_reviewers = $1
        WHERE pr_id = $2
    `, pq.Array(pr.AssignedReviewers), prID)

	if err != nil {
		return nil, "", err
	}
	return &pr, fmt.Sprintf("replaced by %s", newID), nil

}

func StatusMerged(prID string) (*PullRequest, error) {
	var pr PullRequest
	var reviewersBytes []byte
	err := Db.QueryRow(`SELECT pr_id, pr_name, author_id, status, assigned_reviewers, created_at, merged_at
	                    FROM pull_requests
	                    WHERE pr_id = $1`, prID).
		Scan(&pr.PullRequestID, &pr.PullRequestName, &pr.AuthorID, &pr.Status, &reviewersBytes, &pr.CreatedAt, &pr.MergedAt)
	if err == sql.ErrNoRows {
		return nil, errors.New("NOT_FOUND")
	}
	if err != nil {
		return nil, err
	}
	if len(reviewersBytes) > 0 {
		if err := json.Unmarshal(reviewersBytes, &pr.AssignedReviewers); err != nil {
			pr.AssignedReviewers = []string{}
		}
	} else {
		pr.AssignedReviewers = []string{}
	}
	if pr.Status == "MERGED" {
		return nil, errors.New("ALREADY_MERGED")
	}
	pr.Status = "MERGED"
	now := time.Now()
	_, err = Db.Exec(`UPDATE pull_requests SET status = $1, merged_at = $2 WHERE pr_id = $3`,
		pr.Status, now, prID)
	if err != nil {
		return nil, err
	}

	pr.MergedAt = &now
	return &pr, nil
}

func CreatePR(pr *PullRequest) (*PullRequest, error) {
	var exists string
	err := Db.QueryRow(`SELECT pr_id FROM pull_requests WHERE pr_id = $1`, pr.PullRequestID).Scan(&exists)

	if err == nil {

		return nil, errors.New("PR_EXISTS")
	}
	if err != sql.ErrNoRows {
		return nil, err
	}
	var teamPrName string

	err = Db.QueryRow(`select team_name from users where user_id = $1`, pr.AuthorID).Scan(&teamPrName)
	if err == sql.ErrNoRows {
		return nil, errors.New("AUTHOR_NOT_FOUND")
	}
	if err != nil {
		return nil, err
	}
	rows, err := Db.Query(`
		SELECT user_id
		FROM users 
		WHERE team_name = $1 AND is_active = true and user_id != $2`, teamPrName, pr.AuthorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, fmt.Errorf("scan uid: %w", err)
		}
		members = append(members, uid)
	}
	rand.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})

	n := rand.Intn(2) + 1
	if n > len(members) {
		n = len(members)
	}

	selected := members[:n]
	pr.CreatedAt = time.Now()
	pr.Status = "OPEN"
	pr.AssignedReviewers = selected
	_, err = Db.Exec(`
		INSERT INTO pull_requests (
			pr_id, pr_name, author_id, status,
			assigned_reviewers, created_at, merged_at
		) VALUES ($1, $2, $3, $4, $5, $6, NULL)
	`, pr.PullRequestID, pr.PullRequestName, pr.AuthorID,
		pr.Status, pq.Array(selected), pr.CreatedAt)

	if err != nil {
		return nil, err
	}

	return pr, nil
}

func SetIsActive(userID string, isActive bool) (*User, error) {
	var user User
	err := Db.QueryRow(
		`SELECT user_id, username, team_name, is_active 
		 FROM users WHERE user_id = $1`, userID).
		Scan(&user.UserID, &user.Username, &user.TeamName, &user.IsActive)

	if err == sql.ErrNoRows {
		return nil, errors.New("NOT_FOUND")
	}
	if err != nil {
		return nil, err
	}
	line := "update users set is_active = $1 where user_id = $2"
	_, err = Db.Exec(line, isActive, userID)
	if err != nil {
		return nil, err
	}
	user.IsActive = isActive
	return &user, nil

}

func TeamAdd(teamname string, members []TeamMember) (*Team, error) {
	var exists string
	err := Db.QueryRow(`SELECT team_name FROM teams WHERE team_name = $1`, teamname).Scan(&exists)

	if err == nil {

		return nil, errors.New("TEAM_EXISTS")
	}

	if err != sql.ErrNoRows {
		return nil, err
	}

	line := `insert into teams (team_name) values ($1)`
	_, err = Db.Exec(line, teamname)
	if err != nil {
		return nil, err
	}
	for _, value := range members {
		_, err := Db.Exec(`insert into users (user_id, username, team_name, is_active) values($1,$2,$3,$4) on conflict (user_id) do update
            set username = excluded.username,
                team_name = excluded.team_name,
                is_active = excluded.is_active`,
			value.UserID, value.Username, teamname, value.IsActive)
		if err != nil {
			return nil, err
		}
		_, err = Db.Exec(`
            insert into team_members (team_name, user_id)
            values ($1, $2)
            on conflict do nothing
        `, teamname, value.UserID)

		if err != nil {
			return nil, err
		}

	}

	return &Team{
		TeamName: teamname,
		Members:  members,
	}, nil

}
func GetTeam(teamname string) (Team, error) {
	team := Team{}
	err := Db.QueryRow(`select team_name from teams where team_name = $1`, teamname).Scan(&team.TeamName)
	if err != nil {
		return Team{}, err
	}
	rows, err := Db.Query(`
        SELECT u.user_id, u.username, u.is_active FROM users u
        JOIN team_members tm ON tm.user_id = u.user_id
        WHERE tm.team_name = $1
    `, teamname)
	if err != nil {
		return Team{}, err
	}
	defer rows.Close()

	members := []TeamMember{}

	for rows.Next() {
		var m TeamMember
		if err := rows.Scan(&m.UserID, &m.Username, &m.IsActive); err != nil {
			return Team{}, err
		}
		members = append(members, m)
	}

	team.Members = members
	return team, nil

}

func CreateTables() error {
	_, err := Db.Exec(`CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    team_name TEXT,
    is_active BOOLEAN NOT NULL
);
	`)
	if err != nil {
		return err
	}
	_, err = Db.Exec(`
	CREATE TABLE IF NOT EXISTS teams (
    team_name TEXT PRIMARY KEY
	);
`)
	if err != nil {
		return err
	}

	_, err = Db.Exec(`
	CREATE TABLE IF NOT EXISTS team_members (
    team_name TEXT,
    user_id TEXT,
    PRIMARY KEY (team_name, user_id),
    FOREIGN KEY (team_name) REFERENCES teams(team_name) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
	);
`)
	if err != nil {
		return err
	}

	_, err = Db.Exec(`
	CREATE TABLE IF NOT EXISTS pull_requests (
    pr_id TEXT PRIMARY KEY,
    pr_name TEXT NOT NULL,
    author_id TEXT NOT NULL,
    status TEXT NOT NULL,
    assigned_reviewers TEXT[],
    created_at TIMESTAMP NOT NULL,
    merged_at TIMESTAMP,
    FOREIGN KEY (author_id) REFERENCES users(user_id)
	);
`)
	return err
}
