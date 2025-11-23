package api

import (
	dbtablesgo "avito_otbor/dbTablesGo"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func Init(r chi.Router) {
	r.Get("/team/get", teamGetHandle)
	r.Post("/team/add", AddTeamHandle)
	r.Post("/users/setIsActive", SetIsActiveHandle)
	r.Post("/pullRequest/create", PrCreateHandle)
	r.Post("/pullRequest/merge", ChangeStatusHandle)
	r.Post("/pullRequest/reassign", ChangeReviewerHandle)
	r.Get("/users/getReview", GetReviewHandle)
	r.Post("/users/deactivateMany", DeactivateManyHandle)
}

func DeactivateManyHandle(w http.ResponseWriter, r *http.Request) {
	type DeactivateManyRequest struct {
		UserIDs []string `json:"user_ids"`
	}
	var req DeactivateManyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
		return
	}

	if len(req.UserIDs) == 0 {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "user_ids cannot be empty")
		return
	}

	err := dbtablesgo.DeactivateManyUsers(req.UserIDs)
	if err != nil {
		if err.Error() == "NO_REVIEWERS" {
			ErrorJSON(w, http.StatusBadRequest, "NO_REVIEWERS", "no reviewers available")
			return
		}
		ErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to process request")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(
		map[string]string{"status": "ok"}); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func StatsHandle(w http.ResponseWriter, _ *http.Request) {
	stats, err := dbtablesgo.GetStats()
	if err != nil {
		ErrorJSON(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get stats")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func GetReviewHandle(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "id cant be empty")
		return
	}
	pqList, err := dbtablesgo.GetReview(userID)
	if err != nil {
		if err.Error() == "NOT_FOUND" {
			ErrorJSON(w, http.StatusBadRequest, "NOT_FOUND", "user has no assigned PRs")
			return

		}
		ErrorJSON(w, http.StatusBadRequest, "INTERNAL_ERROR", "problems with getting reviews")
		return

	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"user_id":       userID,
		"pull_requests": pqList,
	}); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

}
func ChangeReviewerHandle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PullRequestID string `json:"pull_request_id"`
		OldUserID     string `json:"old_user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "cant read json")
		return
	}
	if body.PullRequestID == "" {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "Id must be not empty")
		return
	}
	updated, replacedBy, err := dbtablesgo.ChangeReviewer(body.PullRequestID, body.OldUserID)
	if err != nil {
		if err.Error() == "NOT_FOUND" {
			ErrorJSON(w, http.StatusBadRequest, "NOT_FOUND", "cant find pr")
			return
		} else if err.Error() == "NOT_ASSIGNED" {
			ErrorJSON(w, http.StatusBadRequest, "NOT_ASSIGNED", "resource not assigned")
			return
		} else if err.Error() == "NO_REPLACEMENT_FOUND" {
			ErrorJSON(w, http.StatusBadRequest, "NO_REPLACEMENT_FOUND", "there is no person to replace")
			return
		}
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "Problems with changing reviewer")
		return

	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"updated":     updated,
		"replaced_by": replacedBy,
	}); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func ChangeStatusHandle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PullRequestID string `json:"pull_request_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "cant read json")
		return
	}
	if body.PullRequestID == "" {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "Id must be not empty")
		return
	}
	updated, err := dbtablesgo.StatusMerged(body.PullRequestID)
	if err != nil {
		if err.Error() == "NOT_FOUND" {
			ErrorJSON(w, http.StatusNotFound, "NOT_FOUND", "PR not found")
			return
		}
		if err.Error() == "ALREADY_MERGED" {
			pr, _ := dbtablesgo.GetPR(body.PullRequestID)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(pr); err != nil {
				http.Error(w, "Failed to encode response", http.StatusInternalServerError)
				return
			}
			return
		}
		ErrorJSON(w, http.StatusInternalServerError, "BAD_REQUEST", "Failed to update PR")
		return

	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(updated); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

}

func PrCreateHandle(w http.ResponseWriter, r *http.Request) {
	var pr dbtablesgo.PullRequest
	err := json.NewDecoder(r.Body).Decode(&pr)
	if err != nil {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "cant read json")
		return
	}
	if pr.PullRequestID == "" || pr.PullRequestName == "" || pr.AuthorID == "" {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "Fields pr_id, pr_name and author_id are required")
		return
	}

	created, err := dbtablesgo.CreatePR(&pr)
	if err != nil {
		if err.Error() == "PR_EXISTS" {
			ErrorJSON(w, http.StatusBadRequest, "PR_EXISTS", "pr is already exists")
			return
		} else if err.Error() == "AUTHOR_NOT_FOUND" {
			ErrorJSON(w, http.StatusBadRequest, "AUTHOR_NOT_FOUND", "there no author")
			return
		}

		ErrorJSON(w, http.StatusInternalServerError, "BAD_REQUEST", "Failed to create PR")
		return

	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(created); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

}

func SetIsActiveHandle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UserID   string `json:"user_id"`
		IsActive bool   `json:"is_active"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "cant read json")
		return
	}

	if body.UserID == "" {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "Id must be not empty")
		return
	}

	updated, err := dbtablesgo.SetIsActive(body.UserID, body.IsActive)
	if err != nil {
		if err.Error() == "NOT_FOUND" {
			ErrorJSON(w, http.StatusBadRequest, "NOT_FOUND", "cant find user")
			return

		}
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "some problems with update activity ")
		return

	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(updated); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func AddTeamHandle(w http.ResponseWriter, r *http.Request) {
	var team dbtablesgo.Team
	err := json.NewDecoder(r.Body).Decode(&team)
	if err != nil {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "Cant Read json")
		return
	}
	if team.TeamName == "" {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "Team name cant be empty")
		return
	}
	if len(team.Members) == 0 {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "There is must be any team member")
		return

	}
	created, err := dbtablesgo.TeamAdd(team.TeamName, team.Members)
	if err != nil {
		if err.Error() == "TEAM_EXISTS" {
			ErrorJSON(w, http.StatusBadRequest, "TEAM_EXISTS", "team is already exist")
			return

		}
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "some problems with Add team ")
		return

	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"team": created,
	}); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

}
func ErrorJSON(w http.ResponseWriter, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	}); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func teamGetHandle(w http.ResponseWriter, r *http.Request) {
	teamname := r.URL.Query().Get("team_name")
	if teamname == "" {
		ErrorJSON(w, http.StatusBadRequest, "BAD_REQUEST", "id cant be empty")
		return
	}
	team, err := dbtablesgo.GetTeam(teamname)

	if err != nil {
		ErrorJSON(w, http.StatusNotFound, "NOT_FOUND", "team not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(team); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

}
