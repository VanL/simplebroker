(.runs[].results[].locations[] | select(
  .physicalLocation.artifactLocation.uri?
    == "no file associated with this alert"
)) |= (
  .physicalLocation.artifactLocation.uri = ".github/workflows/scorecard.yml"
  | .message.text = "Repository-level finding anchored to the Scorecard workflow for SARIF compatibility."
)
