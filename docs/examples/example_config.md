Below is an example ispypsa_config.yaml file, the file configures a simple fast running
model designed to be useful for learning and testing purposes. The model implemented by
the config:

- Optimises capacity expansion to met demand in 2050 in a single build step
- Only considers the NSW region
- Is based on 2024 ISP Step Change scenario assumption
- Retains existing capacity which has not reached retirement age by 2050
- Uses three weeks of timeseries data based on 2018 reference year data the weeks in
2050 with residual peak demand, peak-consumption, and residual minimum demand are
used.

```yaml
--8<-- "ispypsa_config.yaml"
```
