# Status Quo

Single column tests are passing, multicolumn tests are not

I highly suspect the way my step2 stores results is an issue.
I'm storing partial results in NewRecord objects but I think I need to use AggregatorType objects instead.