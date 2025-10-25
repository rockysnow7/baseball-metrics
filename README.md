# baseball-metrics

this is a python library for calculating metrics for mlb players.

## example

```python
from baseball_metrics import Player

# get Shohei Ohtani
player = Player("ohtas001")
# calculate his earned run average over all games from 1/1/2023 to 24/6/2023 (inclusive)
era = player.era(datetime.date(2023, 1, 1), datetime.date(2023, 6, 24))
print(era)
```
