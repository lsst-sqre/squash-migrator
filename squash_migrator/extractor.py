"""Module to extract squash jobs from the old database and write them to
a local directory.
"""


class Extractor:

    def __init__(self, context=None):
        if not context:
            raise RuntimeError("Extractor must be given execution context")
