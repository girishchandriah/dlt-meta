"""
Access control for dlt-meta lakehouse app.

Role-based access control (RBAC) for environment-specific deployments:
- Developers: nonprod only
- Ops Admins: preprod, prod
"""

import logging
import os
from functools import wraps
from flask import request, jsonify
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# Environment access configuration
ROLE_ENVIRONMENT_ACCESS = {
    "developers": ["nonprod"],
    "ops_admins": ["nonprod", "preprod", "prod"],
    "admins": ["nonprod", "preprod", "prod"]  # Full access
}

# Databricks workspace group to role mapping
WORKSPACE_GROUP_TO_ROLE = {
    "dlt-meta-developers": "developers",
    "dlt-meta-ops-admins": "ops_admins",
    "dlt-meta-admins": "admins",
    # Legacy/fallback groups
    "developers": "developers",
    "ops-admins": "ops_admins",
    "admins": "admins"
}


class AccessControl:
    """Manages user authentication and authorization."""

    def __init__(self):
        """Initialize workspace client for user info."""
        self.workspace_client = None
        try:
            # Initialize Databricks SDK client
            # Uses DATABRICKS_HOST and DATABRICKS_TOKEN from environment
            self.workspace_client = WorkspaceClient()
            logger.info("Databricks WorkspaceClient initialized successfully")
        except Exception as e:
            logger.warning(f"Could not initialize Databricks client: {e}")
            logger.warning("Access control will be disabled - all users have full access")

    def get_current_user(self):
        """Get current authenticated user email."""
        if not self.workspace_client:
            return None

        try:
            user = self.workspace_client.current_user.me()
            return user.user_name  # Returns email address
        except Exception as e:
            logger.error(f"Failed to get current user: {e}")
            return None

    def get_user_groups(self, user_email):
        """Get all workspace groups for a user."""
        if not self.workspace_client or not user_email:
            return []

        try:
            # Get user ID from email
            users = list(self.workspace_client.users.list(filter=f'userName eq "{user_email}"'))
            if not users:
                logger.warning(f"User not found: {user_email}")
                return []

            user_id = users[0].id

            # Get groups for user
            groups = list(self.workspace_client.groups.list(filter=f'members.value eq "{user_id}"'))
            group_names = [group.display_name for group in groups]

            logger.info(f"User {user_email} is in groups: {group_names}")
            return group_names
        except Exception as e:
            logger.error(f"Failed to get user groups: {e}")
            return []

    def get_user_role(self, user_email):
        """Determine user's role based on workspace group membership."""
        if not user_email:
            return None

        groups = self.get_user_groups(user_email)

        # Check each group against our mapping
        for group in groups:
            if group in WORKSPACE_GROUP_TO_ROLE:
                role = WORKSPACE_GROUP_TO_ROLE[group]
                logger.info(f"User {user_email} has role: {role} (from group: {group})")
                return role

        # No matching group found
        logger.warning(f"User {user_email} is not in any dlt-meta access groups")
        return None

    def get_allowed_environments(self, user_email):
        """Get list of environments user can access."""
        role = self.get_user_role(user_email)
        if not role:
            # No role = no access (or fallback to nonprod only for safety)
            return ["nonprod"]

        return ROLE_ENVIRONMENT_ACCESS.get(role, [])

    def can_access_environment(self, user_email, environment):
        """Check if user can access specific environment."""
        if not self.workspace_client:
            # If access control is disabled (no client), allow all
            logger.warning("Access control disabled - allowing access")
            return True

        allowed_envs = self.get_allowed_environments(user_email)
        can_access = environment.lower() in [env.lower() for env in allowed_envs]

        if not can_access:
            logger.warning(
                f"Access denied: User {user_email} tried to access {environment} "
                f"but is only allowed: {allowed_envs}"
            )

        return can_access


# Global access control instance
access_control = AccessControl()


def require_environment_access(environment_param='environment'):
    """
    Decorator to enforce environment-based access control.

    Args:
        environment_param: Name of the form parameter containing environment name

    Usage:
        @app.route('/onboarding', methods=['POST'])
        @require_environment_access('environment')
        def handle_onboard_form():
            ...
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get environment from request
            if request.method == 'POST':
                if request.is_json:
                    environment = request.json.get(environment_param)
                else:
                    environment = request.form.get(environment_param)
            else:
                environment = request.args.get(environment_param)

            if not environment:
                return jsonify({
                    'error': 'Environment parameter is required',
                    'status': 'error'
                }), 400

            # Get current user
            user_email = access_control.get_current_user()
            if not user_email:
                # If we can't determine user, deny access for safety
                # (unless access control is disabled)
                if access_control.workspace_client:
                    return jsonify({
                        'error': 'Unable to authenticate user',
                        'status': 'error'
                    }), 401

            # Check access
            if not access_control.can_access_environment(user_email, environment):
                allowed_envs = access_control.get_allowed_environments(user_email)
                return jsonify({
                    'error': f'Access denied: You do not have permission to deploy to {environment}',
                    'allowed_environments': allowed_envs,
                    'status': 'error'
                }), 403

            # Access granted - proceed with original function
            return f(*args, **kwargs)

        return decorated_function
    return decorator


def get_user_context():
    """
    Get current user context for UI rendering.

    Returns:
        dict with keys: user_email, role, allowed_environments
    """
    user_email = access_control.get_current_user()
    if not user_email:
        return {
            'user_email': 'unknown',
            'role': None,
            'allowed_environments': ['nonprod']  # Default safe fallback
        }

    role = access_control.get_user_role(user_email)
    allowed_envs = access_control.get_allowed_environments(user_email)

    return {
        'user_email': user_email,
        'role': role,
        'allowed_environments': allowed_envs
    }
