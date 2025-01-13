from python_estudo.models.base_feature_controller import BaseFeatureController
from .empty_page import EmptyPage

def fDefaultButtons():
    return {
        "button1": {"text": "Empty page", "userActionName": "changeToEmptyPage"},
    }

def fPagesDict():
    from .empty_page import EmptyPage
    return {
        "empty_page": EmptyPage,
    }

class TestsController(BaseFeatureController):
    class UserActions(BaseFeatureController.UserActions):
        def getActionsDict(self):
            # override for test feature
            return {
                "changeToEmptyPage": self.changeToEmptyPage,
            }
        def changeToEmptyPage(self):
            self.controller.changePage("empty_page")
    
    def __init__(self, style=None, firstPage=EmptyPage):
        super().__init__(
            style=style,
            defaultPage=firstPage,
            pagesDict=fPagesDict(),
            buttonsData=fDefaultButtons(),
        )
    
    def provideFeatureUi(self, parent):
        from .tests_main_ui import TestsMainUi
        return TestsMainUi(controller=self, parent=parent)