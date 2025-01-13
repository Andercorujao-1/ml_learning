from dataclasses import dataclass

@dataclass
class UiButtonDataModel:
    text: str
    userActionName: str
    userActionFunction: callable

    def __init__(self, text, userActionName, userActionFunction=None):
        self.text = text
        self.userActionName = userActionName
        self.userActionFunction = userActionFunction



    def setUserActionFunction(self, actionFunctionDict: dict[str, callable]):
        try:
            self.userActionFunction = actionFunctionDict[self.userActionName]
        except KeyError:
            raise KeyError(f"Action {self.userActionName} not found in actionFunctionDict")
        
    @staticmethod
    def factoryFromJson(data: dict, actionFunctionDict: dict[str, callable] = None) -> dict[str, 'UiButtonDataModel']:
        buttonDataModels = {}
        for (key, buttonDataDict) in data.items():
            try:
                buttonDataModel = UiButtonDataModel(**buttonDataDict)
            except TypeError:
                raise TypeError(f"Invalid data for buttonDataModel {key}")
            
            if actionFunctionDict is not None:
                buttonDataModel.setUserActionFunction(actionFunctionDict)
            buttonDataModels[key] = buttonDataModel

        return buttonDataModels





if __name__ == "__main__":
    data = {
        "button1": {
            "text": "Click Me1",
            "userActionName": "print1"
        },
        "button2": {
            "text": "Click Me2",
            "userActionName": "print2"
        }
    }
    def print1():
        print("Button 1 Clicked")
    def print2():
        print("Button 2 Clicked")

    actionFunctionDict = {
        "print1": print1,
        "print2": print2
    }

    buttonDataModels = UiButtonDataModel.factoryFromJson(data, actionFunctionDict)
    for (key, buttonDataModel) in buttonDataModels.items():
        print(buttonDataModel.text)
        buttonDataModel.userActionFunction()