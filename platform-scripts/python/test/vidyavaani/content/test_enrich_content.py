from enrich_content import createDirectory, enrichContent
import pytest

def test_enrichContent():
	contentJSON = {}
	enrichContent(contentJSON)
	pass