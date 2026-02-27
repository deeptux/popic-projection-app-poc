import { createSelector, createFeatureSelector } from '@ngrx/store';
import type { SpreadsheetsState } from './spreadsheets.reducer';

export const selectSpreadsheetsState =
  createFeatureSelector<SpreadsheetsState>('spreadsheets');

export const selectRawFileResults = createSelector(
  selectSpreadsheetsState,
  state => state.rawFileResults
);

export const selectCleanedFileResults = createSelector(
  selectSpreadsheetsState,
  state => state.cleanedFileResults
);

export const selectSelectedRawIndex = createSelector(
  selectSpreadsheetsState,
  state => state.selectedRawIndex
);

export const selectSelectedCleanedIndex = createSelector(
  selectSpreadsheetsState,
  state => state.selectedCleanedIndex
);

export const selectCommissionRawFileResults = createSelector(
  selectSpreadsheetsState,
  state => state.commissionRawFileResults
);

export const selectCommissionCleanedFileResults = createSelector(
  selectSpreadsheetsState,
  state => state.commissionCleanedFileResults
);

export const selectSelectedCommissionRawIndex = createSelector(
  selectSpreadsheetsState,
  state => state.selectedCommissionRawIndex
);

export const selectSelectedCommissionCleanedIndex = createSelector(
  selectSpreadsheetsState,
  state => state.selectedCommissionCleanedIndex
);

export const selectHasAnyCommissionRawResult = createSelector(
  selectCommissionRawFileResults,
  slots => slots.length > 0
);

export const selectCommissionRawSlotCount = createSelector(
  selectCommissionRawFileResults,
  slots => slots.length
);

export const selectCommissionCleanedSlotCount = createSelector(
  selectCommissionCleanedFileResults,
  slots => slots.length
);

export const selectHasAnyRawResult = createSelector(
  selectRawFileResults,
  slots => slots.length > 0
);

export const selectRawSlotCount = createSelector(
  selectRawFileResults,
  slots => slots.length
);

export const selectCleanedSlotCount = createSelector(
  selectCleanedFileResults,
  slots => slots.length
);
