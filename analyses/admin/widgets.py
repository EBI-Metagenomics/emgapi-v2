from django_json_widget.widgets import JSONEditorWidget


class ENAAccessionsListWidget(JSONEditorWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(
            options={"mode": "tree", "mainMenuBar": False, "modes": ["tree"]},
            height="100px",
            width="30em",
            *args,
            **kwargs,
        )


class JSONTreeWidget(JSONEditorWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(options={"mode": "tree"}, *args, **kwargs)
