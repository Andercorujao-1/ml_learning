from dataclasses import dataclass


@dataclass
class UiButtonDataModel:
    text: str
    userActionName: str
    userActionFunction: callable = None

    def __init__(self, text: str, userActionName: str, userActionFunction: callable = None) -> None:
        self.text = text
        self.userActionName = userActionName
        self.userActionFunction = userActionFunction

    def fSetUserActionFunction(self, actionFunctionDict: dict[str, callable]) -> None:
        try:
            self.userActionFunction = actionFunctionDict[self.userActionName]
            logger.debug(f"Action function set for {self.userActionName}.")
        except KeyError as e:
            logger.error(f"Action {self.userActionName} not found in actionFunctionDict: {e}")
            raise KeyError(f"Action {self.userActionName} not found in actionFunctionDict") from e

    @staticmethod
    def fFactoryFromJson(data: dict, actionFunctionDict: dict[str, callable] = None) -> dict[str, "UiButtonDataModel"]:
        buttonDataModels = {}
        for key, buttonDataDict in data.items():
            try:
                buttonDataModel = UiButtonDataModel(**buttonDataDict)
            except TypeError as e:
                logger.error(f"Invalid data for UiButtonDataModel {key}: {e}")
                raise TypeError(f"Invalid data for UiButtonDataModel {key}") from e

            if actionFunctionDict is not None:
                buttonDataModel.fSetUserActionFunction(actionFunctionDict)
            buttonDataModels[key] = buttonDataModel
            logger.debug(f"Button data model created for key: {key}")
        return buttonDataModels

# --- Basic test ---
if __name__ == "__main__":
    data = {
        "button1": {"text": "Click Me1", "userActionName": "fPrint1"},
        "button2": {"text": "Click Me2", "userActionName": "fPrint2"},
    }

    def fPrint1():
        print("Button 1 Clicked")

    def fPrint2():
        print("Button 2 Clicked")

    actionFunctionDict = {"fPrint1": fPrint1, "fPrint2": fPrint2}

    buttonDataModels = UiButtonDataModel.fFactoryFromJson(data, actionFunctionDict)
    for key, buttonDataModel in buttonDataModels.items():
        print(buttonDataModel.text)
        buttonDataModel.userActionFunction()
