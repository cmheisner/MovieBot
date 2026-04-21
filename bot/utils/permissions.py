from __future__ import annotations

import discord


def user_has_staff_role(user: discord.abc.User, staff_role_id: int) -> bool:
    """Accept either the numeric role ID or a role literally named 'Staff' (fallback)."""
    roles = getattr(user, "roles", []) or []
    return any(
        (staff_role_id and r.id == staff_role_id) or r.name.lower() == "staff"
        for r in roles
    )
