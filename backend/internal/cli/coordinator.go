package cli

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type externalCoordinatorDTO struct {
	ProjectID   string    `json:"projectId"`
	OwnerID     string    `json:"ownerId"`
	DisplayName string    `json:"displayName"`
	LastSeenAt  time.Time `json:"lastSeenAt"`
	ExpiresAt   time.Time `json:"expiresAt"`
}

type externalCoordinatorResponse struct {
	ExternalCoordinator *externalCoordinatorDTO `json:"externalCoordinator"`
}

func newCoordinatorCommand(ctx *commandContext) *cobra.Command {
	cmd := &cobra.Command{Use: "coordinator", Short: "Manage an external project coordinator lease"}
	cmd.AddCommand(newCoordinatorClaimCommand(ctx), newCoordinatorHeartbeatCommand(ctx), newCoordinatorStatusCommand(ctx), newCoordinatorClearCommand(ctx))
	return cmd
}

func newCoordinatorHeartbeatCommand(ctx *commandContext) *cobra.Command {
	var project, owner string
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "heartbeat",
		Short: "Renew an existing external coordinator lease for five minutes",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(project) == "" || strings.TrimSpace(owner) == "" {
				return usageError{errors.New("usage: --project and --owner are required")}
			}
			path := coordinatorPath(project)
			var current externalCoordinatorResponse
			if err := ctx.getJSON(cmd.Context(), path, &current); err != nil {
				return err
			}
			if current.ExternalCoordinator == nil || current.ExternalCoordinator.OwnerID != owner {
				return errors.New("no live external coordinator lease is owned by this owner")
			}
			var renewed externalCoordinatorResponse
			if err := ctx.putJSON(cmd.Context(), path, map[string]string{
				"ownerId": owner, "displayName": current.ExternalCoordinator.DisplayName,
			}, &renewed); err != nil {
				return err
			}
			return writeCoordinatorResponse(cmd, renewed, jsonOutput)
		},
	}
	cmd.Flags().StringVar(&project, "project", "", "Project ID")
	cmd.Flags().StringVar(&owner, "owner", "", "Owner ID used to claim")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output as JSON")
	return cmd
}

func newCoordinatorClaimCommand(ctx *commandContext) *cobra.Command {
	var project, owner, name string
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "claim",
		Short: "Claim or renew an external coordinator lease for five minutes",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(project) == "" || strings.TrimSpace(owner) == "" || strings.TrimSpace(name) == "" {
				return usageError{errors.New("usage: --project, --owner, and --name are required")}
			}
			var res externalCoordinatorResponse
			if err := ctx.putJSON(cmd.Context(), coordinatorPath(project), map[string]string{
				"ownerId": owner, "displayName": name,
			}, &res); err != nil {
				return err
			}
			return writeCoordinatorResponse(cmd, res, jsonOutput)
		},
	}
	cmd.Flags().StringVar(&project, "project", "", "Project ID")
	cmd.Flags().StringVar(&owner, "owner", "", "Stable owner ID; use the same value to renew or clear")
	cmd.Flags().StringVar(&name, "name", "", "Display name, for example Codex Desktop")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output as JSON")
	return cmd
}

func newCoordinatorStatusCommand(ctx *commandContext) *cobra.Command {
	var project string
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the live external coordinator lease",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(project) == "" {
				return usageError{errors.New("usage: --project is required")}
			}
			var res externalCoordinatorResponse
			if err := ctx.getJSON(cmd.Context(), coordinatorPath(project), &res); err != nil {
				return err
			}
			return writeCoordinatorResponse(cmd, res, jsonOutput)
		},
	}
	cmd.Flags().StringVar(&project, "project", "", "Project ID")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output as JSON")
	return cmd
}

func newCoordinatorClearCommand(ctx *commandContext) *cobra.Command {
	var project, owner string
	cmd := &cobra.Command{
		Use:   "clear",
		Short: "Clear a lease owned by this external coordinator",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(project) == "" || strings.TrimSpace(owner) == "" {
				return usageError{errors.New("usage: --project and --owner are required")}
			}
			path := coordinatorPath(project) + "?ownerId=" + url.QueryEscape(owner)
			if err := ctx.deleteJSON(cmd.Context(), path, nil); err != nil {
				return err
			}
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "External coordinator cleared")
			return err
		},
	}
	cmd.Flags().StringVar(&project, "project", "", "Project ID")
	cmd.Flags().StringVar(&owner, "owner", "", "Owner ID used to claim")
	return cmd
}

func coordinatorPath(project string) string {
	return "projects/" + url.PathEscape(strings.TrimSpace(project)) + "/external-coordinator"
}

func writeCoordinatorResponse(cmd *cobra.Command, res externalCoordinatorResponse, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(cmd.OutOrStdout(), res)
	}
	if res.ExternalCoordinator == nil {
		_, err := fmt.Fprintln(cmd.OutOrStdout(), "(no live external coordinator)")
		return err
	}
	_, err := fmt.Fprintf(cmd.OutOrStdout(), "%s (%s), lease expires %s\n",
		res.ExternalCoordinator.DisplayName, res.ExternalCoordinator.OwnerID,
		res.ExternalCoordinator.ExpiresAt.Format(time.RFC3339))
	return err
}
