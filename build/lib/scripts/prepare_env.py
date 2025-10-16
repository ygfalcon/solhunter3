from solhunter_zero.env_config import configure_environment


def main() -> None:
    """Ensure a .env file exists with sane defaults."""
    configure_environment()


if __name__ == "__main__":
    main()
