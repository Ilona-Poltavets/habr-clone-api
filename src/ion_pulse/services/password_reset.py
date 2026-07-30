import asyncio
import logging
import smtplib
from email.message import EmailMessage
from urllib.parse import quote

from ion_pulse.core.config import get_settings

logger = logging.getLogger(__name__)


async def deliver_password_reset(email: str, token: str) -> None:
    """Deliver a recovery URL without persisting or returning the raw token."""
    settings = get_settings()
    reset_url = f"{settings.site_url.rstrip('/')}/reset-password?token={quote(token)}"
    if settings.password_reset_delivery == "log":
        logger.warning("Password recovery link for %s: %s", email, reset_url)
        return
    if not settings.smtp_host:
        logger.error("Password reset SMTP delivery is enabled but ION_PULSE_SMTP_HOST is missing")
        return
    await asyncio.to_thread(_send_smtp, email, reset_url)


def _send_smtp(recipient: str, reset_url: str) -> None:
    settings = get_settings()
    if settings.smtp_host is None:
        raise ValueError("SMTP host is required for password reset delivery")
    message = EmailMessage()
    message["Subject"] = "Восстановление доступа к Ion Pulse"
    message["From"] = settings.smtp_from_email
    message["To"] = recipient
    message.set_content(
        "Чтобы задать новый пароль Ion Pulse, откройте ссылку в течение "
        f"{settings.password_reset_lifetime_minutes} минут:\n\n{reset_url}\n\n"
        "Если запрос сделали не вы, просто проигнорируйте это письмо."
    )
    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as smtp:
        if settings.smtp_use_tls:
            smtp.starttls()
        if settings.smtp_username and settings.smtp_password:
            smtp.login(settings.smtp_username, settings.smtp_password)
        smtp.send_message(message)
