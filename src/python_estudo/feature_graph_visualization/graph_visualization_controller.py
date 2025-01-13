from python_estudo.models.base_feature_controller import BaseFeatureController
from .visual_interface.example_graph import GraphPage
from .visual_interface.example_table import TablePage

def fDefaultButtons():
    return {
        "button1": {"text": "Table page", "userActionName": "changeToTablePage"},
        "button2": {"text": "Graph page", "userActionName": "changeToGraphPage"},
    }

def fPagesDict():
    return {
        "table_page": TablePage,
        "graph_page": GraphPage,
    }

class GraphVisualizationController(BaseFeatureController):
    class UserActions(BaseFeatureController.UserActions):
        def getActionsDict(self):
            # override to provide custom user actions
            return {
                "changeToTablePage": self.changeToTablePage,
                "changeToGraphPage": self.changeToGraphPage,
            }
        
        def changeToTablePage(self):
            self.controller.changePage("table_page")

        def changeToGraphPage(self):
            self.controller.changePage("graph_page")
    
    def __init__(self, style=None, firstPage=TablePage):
        super().__init__(
            style=style,
            defaultPage=firstPage,
            pagesDict=fPagesDict(),
            buttonsData=fDefaultButtons()
        )
    
    def provideFeatureUi(self, parent):
        """Return the main UI frame for graph visualization."""
        from .graph_visualization_main_ui import GraphVisualizationMainUi
        return GraphVisualizationMainUi(controller=self, parent=parent)