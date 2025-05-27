## Description

<!--
Please do not leave this blank
This PR [adds/removes/fixes/replaces] the [feature/bug/etc].
-->

## Related Tickets & Documents

- DENG-XXXX
- DSRE-XXXX

<!--
Please reference related Jira tickets, GitHub issues or Bugzilla. This repo has been
configured to automatically insert hyperlinks for DSRE and DENG tickets.
See https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/managing-repository-settings/configuring-autolinks-to-reference-external-resources
-->

## Merging Guidelines

- Only merge changes to `main` that you want to propagate to production automatically
- Check [pipeline latency](https://yardstick.mozilla.org/d/bZHv1mUMk/pipeline-latency?orgId=1&from=now-6h&to=now) before merging, particularly if:
  - The merge will occur within 2 hours of the UTC date change, or
  - You are merging multiple PRs in quick suggestion

See the full [code deployment policy](https://mozilla-hub.atlassian.net/wiki/spaces/SRE/pages/27922303/Ingestion+Beam#Prod-Code-Deployment-Policy)
for details.
