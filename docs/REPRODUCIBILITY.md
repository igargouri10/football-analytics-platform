# Reproducibility and Experiment Guide

## Main workflow outputs
- generation summary
- merged schema
- multibatch anomaly summary
- usefulness audit
- runtime logs
- research summary

## Comparator interpretation
Three executable comparator conditions are used:
1. weak manual-only baseline
2. manual-expanded comparator
3. manual-plus-LLM

Expected main comparator result:
- manual-only: 7 / 16
- manual-expanded: 16 / 16
- manual-plus-LLM: 16 / 16

## C5 stability protocol

### Frozen-schema
Re-run the comparator while keeping the merged LLM schema fixed.

### Fresh-generation
Regenerate LLM tests from scratch, merge them, then re-run the comparator.

Expected stability outcome:
- manual-only stays at 7 / 16
- manual-expanded stays at 16 / 16
- manual-plus-LLM stays at 16 / 16

## Important note
The main workflow usefulness counts and the fresh-generation C5 usefulness counts may differ.
Document them separately rather than collapsing them into one number.
