<p align="center">
    <a alt="License"
        href="https://github.com/brooklyn-data/dbt_klaviyo/blob/main/LICENSE.md">
        <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" /></a>
    <a alt="dbt-core">
        <img src="https://img.shields.io/badge/dbt_Core™_version->=1.3.0_,<2.0.0-orange.svg" /></a>
    <a alt="Maintained?">
        <img src="https://img.shields.io/badge/Maintained%3F-yes-green.svg" /></a>
    <a alt="PRs">
        <img src="https://img.shields.io/badge/Contributions-welcome-blueviolet" /></a>
</p>

# Klaviyo Meltano tap data transformation for dbt

This package is designed to work alongside the [Klaviyo Meltano tap](https://github.com/brooklyn-data/tap-klaviyo) to transform the data into an easy-to-use format.

Supported warehouses:

- Snowflake

## Installation and use

1. Include the following package version in your `packages.yml` file. Check [dbt Hub](https://hub.getdbt.com/) for the latest version to use.

```yml
packages:
    - package: meltano/klaviyo # TBC
      version: 0.1.0 # TBC
```

2. Install the package by running `dbt deps`

3. In your `dbt_project.yml` file, include the locations where the tap data is expected to load from:

```yml
vars:
  klaviyo_database: your_database_name # By default this is `klaviyo`
  klaviyo_schema: your_database_schema # By default this is `tap_klaviyo`
```

4. Choose which schemas you want to build the tables in by including this in your `dbt_project.yml` file:

```yml
models:
  dbt_klaviyo:
    marts:
      +schema: your_marts_schema # By default this is `klaviyo`
    staging:
      +schema: your_staging_schema # By default this is `stg_klaviyo`
```

5. Build the tables by running

```sh
dbt run --select package:dbt_klaviyo
```

## Tables

These are the tables that will be built with this project.

| Table | Description |
| ----- | ----------- |
| stg_klaviyo__campaigns | List of campaigns from the account |
| stg_klaviyo__person | List of profiles from the account |
| stg_klaviyo__events | List of events in the account |
| stg_klaviyo__event_items | List of items associated with events. Joins back to the events table through event_id |
| stg_klaviyo__flows | List of flows in the account |
| stg_klaviyo__metrics | List of metrics from the account |
| stg_klaviyo__integrations | List of account integrations, from the metrics endpoint |
| stg_klaviyo__lists | List of account lists |
| stg_klaviyo__listperson | Mapping table between lists and profiles |
| stg_klaviyo__email_template | List of email templates |

## Contributing

Want to get involved? Please do! Feel free to create your own fork and create PRs with updates. If you aren't sure how to get started, then please reach out to one of the maintainers, and we'd be happy to help.

Spot something amiss? Please create an issue on [GitHub](https://github.com/brooklyn-data/dbt_klaviyo/issues).
