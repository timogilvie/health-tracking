from pathlib import Path


def test_legal_documents_cover_local_privacy_and_no_warranties(
    project_root: Path,
) -> None:
    privacy = (project_root / "PRIVACY.md").read_text(encoding="utf-8")
    terms = (project_root / "TERMS.md").read_text(encoding="utf-8")
    readme = (project_root / "README.md").read_text(encoding="utf-8")
    privacy_text = " ".join(privacy.split())
    terms_text = " ".join(terms.split())

    for statement in (
        "do not collect, receive, use, sell, rent, monetize, or share your health",
        "There are no project-operated data servers",
        "does not transmit health data to the project maintainers",
        "Oura data is not shared with any third party",
        "Your choices: access, revocation, and deletion",
    ):
        assert statement in privacy_text

    for statement in (
        "Informational purposes only",
        "is not a medical device",
        "No warranties",
        "AS IS",
        "Limitation of liability",
        "third-party products and service providers are disclaimed from all warranties",
    ):
        assert statement in terms_text

    assert "[Terms of Service](TERMS.md)" in readme
    assert "[Privacy Policy](PRIVACY.md)" in readme
