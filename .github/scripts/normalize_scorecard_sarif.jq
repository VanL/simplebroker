(.runs[].results[] | select(has("locations"))) |= (
  .locations |= map(
    select(
      .physicalLocation.artifactLocation.uri?
        != "no file associated with this alert"
    )
  )
  | if .locations == [] then del(.locations) else . end
)
