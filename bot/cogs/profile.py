from __future__ import annotations

import json
import logging
import pathlib

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)

IMAGES_DIR = pathlib.Path(__file__).parent.parent / "images"
STATE_FILE = IMAGES_DIR / "profile_state.json"

REAL_IMAGE = IMAGES_DIR / "Real.jpeg"
TOON_IMAGE = IMAGES_DIR / "Toon.jpeg"


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"base": "real", "override": False}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state))


class ProfileCog(commands.Cog):

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    profile = app_commands.Group(
        name="profile",
        description="Manage the bot's profile image.",
    )

    @profile.command(name="real", description="Set the bot profile to the real photo.")
    async def profile_real(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            img = REAL_IMAGE.read_bytes()
            await self.bot.user.edit(avatar=img)
            state = _load_state()
            state["base"] = "real"
            state["override"] = False
            _save_state(state)
            log.info("Profile set to Real by %s.", interaction.user)
            await interaction.followup.send("✅ Profile set to **Real**.", ephemeral=True)
        except discord.HTTPException as exc:
            await interaction.followup.send(
                f"⚠️ Discord rate-limited avatar changes. Try again later. ({exc})", ephemeral=True
            )

    @profile.command(name="toon", description="Set the bot profile to the cartoon image.")
    async def profile_toon(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            img = TOON_IMAGE.read_bytes()
            await self.bot.user.edit(avatar=img)
            state = _load_state()
            state["base"] = "toon"
            state["override"] = False
            _save_state(state)
            log.info("Profile set to Toon by %s.", interaction.user)
            await interaction.followup.send("✅ Profile set to **Toon**.", ephemeral=True)
        except discord.HTTPException as exc:
            await interaction.followup.send(
                f"⚠️ Discord rate-limited avatar changes. Try again later. ({exc})", ephemeral=True
            )

    @profile.command(
        name="upload",
        description="Override the profile with a custom image until the next event ends.",
    )
    @app_commands.describe(image="Image file to use as the bot's profile picture.")
    async def profile_upload(
        self, interaction: discord.Interaction, image: discord.Attachment
    ) -> None:
        await interaction.response.defer(ephemeral=True)
        if not image.content_type or not image.content_type.startswith("image/"):
            await interaction.followup.send("⚠️ Please attach an image file.", ephemeral=True)
            return
        try:
            img_bytes = await image.read()
            await self.bot.user.edit(avatar=img_bytes)
            state = _load_state()
            state["override"] = True
            _save_state(state)
            log.info("Profile overridden by %s via upload.", interaction.user)
            await interaction.followup.send(
                "✅ Profile overridden. It will reset after the next event ends.", ephemeral=True
            )
        except discord.HTTPException as exc:
            await interaction.followup.send(
                f"⚠️ Discord rate-limited avatar changes. Try again later. ({exc})", ephemeral=True
            )

    @profile.command(
        name="reset",
        description="Clear any override and return to the current real/toon base image.",
    )
    async def profile_reset(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        await self.reset_if_override(force=True)
        state = _load_state()
        base = state.get("base", "real")
        await interaction.followup.send(
            f"✅ Profile reset to **{'Real' if base == 'real' else 'Toon'}**.", ephemeral=True
        )

    async def reset_if_override(self, force: bool = False) -> None:
        """
        Called by the event-end listener. If an override is active (or force=True),
        reset the bot avatar back to the current base image.
        """
        state = _load_state()
        if not force and not state.get("override"):
            return
        base = state.get("base", "real")
        image_path = REAL_IMAGE if base == "real" else TOON_IMAGE
        try:
            img = image_path.read_bytes()
            await self.bot.user.edit(avatar=img)
            state["override"] = False
            _save_state(state)
            log.info("Profile reset to %s after event end.", base)
        except discord.HTTPException as exc:
            log.warning("Profile reset failed (rate limited?): %s", exc)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ProfileCog(bot))
