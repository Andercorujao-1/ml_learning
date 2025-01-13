from python_estudo.models.changenotifier import ChangeNotifier
from python_estudo.models.ui_button_data_model import UiButtonDataModel
from typing import Dict, Callable

class BaseFeatureController:
    """A base class for all feature controllers to reduce duplication."""
    
    def __init__(self, style=None, defaultPage=None, pagesDict=None, buttonsData=None):
        self.style = style
        self.pageUi = None
        self.buttonDataModels = None
        
        # For page switching
        self.pageObject = ChangeNotifier(defaultPage)

        # Subclasses typically define or pass these:
        self.pagesDict = pagesDict or {}
        self.buttonsData = buttonsData or {}
        self.userActions = self.UserActions(self)  # or self.createUserActions()

    def provideFeatureUi(self, parent):
        """Subclasses must implement or override to return their main UI frame."""
        raise NotImplementedError("Subclasses must implement provideFeatureUi()")

    def provideButtonDataObjects(self) -> Dict[str, UiButtonDataModel]:
        """Return a dictionary of button data objects (caching them)."""
        if self.buttonDataModels is not None:
            return self.buttonDataModels
        else:
            userActionsDict = self.userActions.getActionsDict()
            self.buttonDataModels = UiButtonDataModel.factoryFromJson(
                self.buttonsData, userActionsDict
            )
            return self.buttonDataModels

    def buildPageUi(self, pageArea):
        self.pageArea = pageArea
        self.pageObject.addObserver(self._applyPageUi)
        self._applyPageUi()

    def _applyPageUi(self):
        """Destroys old page and creates the new one."""
        if self.pageUi is not None:
            self.pageUi.destroy()
        pageClass = self.pageObject.getValue()  # The class of the page (like TablePage)
        self.pageUi = pageClass(controller=self, parent=self.pageArea)
        self.pageUi.pack(fill="both", expand=True)

    def changePage(self, pageName: str):
        """Change the current page if different from the existing one."""
        nextPageClass = self.pagesDict[pageName]
        if isinstance(self.pageUi, nextPageClass):
            print("Already on this page:", pageName)
            return
        self.pageObject.changeValue(nextPageClass)
    
    class UserActions:
        """Nested class that manages user actions / calls back into the controller."""
        def __init__(self, controller: 'BaseFeatureController'):
            self.controller = controller
        
        def getActionsDict(self) -> Dict[str, Callable]:
            # Subclasses override or extend to add custom user actions
            return {}