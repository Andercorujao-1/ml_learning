from emater_data_science.main_navigation_control.navigation_control_main_controller import (
    NavigationControlMainController,
)

from emater_data_science.logging.log_in_disk import LogInDisk
from emater_data_science.data.data_interface import DataInterface


def main() -> None:
    try:
        mainController = NavigationControlMainController()
        mainController.fLaunchUi()
    except Exception as e:
        LogInDisk().log(level="ERROR", message=str(e))
        raise
    finally:
        DataInterface.fShutdown()


if __name__ == "__main__":
    main()
