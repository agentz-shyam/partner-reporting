import json


class mailtDetails:
    def __init__(self, toId, bccIds, htmlContent):
        self.toIds = toId

        self.bccIds = bccIds

        self.fromDisplayName = "Admin<alerts@agentz.ai>"

        self.sessionId = "ETL alert mail"

        self.body = htmlContent

        self.subject = "ETL process summary"

        self.reason = "MODEL_ALERT"

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
