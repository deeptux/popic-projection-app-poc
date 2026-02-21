import { createReducer, on } from '@ngrx/store';
import type { FileResultSlot } from './spreadsheets.actions';
import {
  setRawSlots,
  setCleanedSlots,
  rawFileSuccess,
  rawFileError,
  cleanedFileSuccess,
  cleanedFileError,
  setSelectedRawIndex,
  setSelectedCleanedIndex,
  resetSpreadsheetsUpload
} from './spreadsheets.actions';

export interface SpreadsheetsState {
  rawFileResults: FileResultSlot[];
  cleanedFileResults: FileResultSlot[];
  selectedRawIndex: number;
  selectedCleanedIndex: number;
}

const initialState: SpreadsheetsState = {
  rawFileResults: [],
  cleanedFileResults: [],
  selectedRawIndex: 0,
  selectedCleanedIndex: 0
};

export const spreadsheetsReducer = createReducer(
  initialState,
  on(setRawSlots, (state, { filenames }) => ({
    ...state,
    rawFileResults: filenames.map(filename => ({
      filename,
      loading: true
    })),
    selectedRawIndex: 0
  })),
  on(rawFileSuccess, (state, { index, result }) => {
    const next = [...state.rawFileResults];
    if (next[index]) {
      next[index] = {
        filename: result.filename,
        loading: false,
        data: result.data,
        columns: result.columns
      };
    }
    return { ...state, rawFileResults: next };
  }),
  on(rawFileError, (state, { index, error }) => {
    const next = [...state.rawFileResults];
    if (next[index]) {
      next[index] = {
        ...next[index],
        loading: false,
        error: error || 'Upload failed'
      };
    }
    return { ...state, rawFileResults: next };
  }),
  on(setCleanedSlots, (state, { filenames }) => ({
    ...state,
    cleanedFileResults: filenames.map(filename => ({
      filename,
      loading: true
    })),
    selectedCleanedIndex: 0
  })),
  on(cleanedFileSuccess, (state, { index, result }) => {
    const next = [...state.cleanedFileResults];
    if (next[index]) {
      next[index] = {
        filename: result.filename,
        loading: false,
        data: result.data,
        columns: result.columns
      };
    }
    return { ...state, cleanedFileResults: next };
  }),
  on(cleanedFileError, (state, { index, error }) => {
    const next = [...state.cleanedFileResults];
    if (next[index]) {
      next[index] = {
        ...next[index],
        loading: false,
        error: error || 'Upload failed'
      };
    }
    return { ...state, cleanedFileResults: next };
  }),
  on(setSelectedRawIndex, (state, { index }) => ({
    ...state,
    selectedRawIndex: index
  })),
  on(setSelectedCleanedIndex, (state, { index }) => ({
    ...state,
    selectedCleanedIndex: index
  })),
  on(resetSpreadsheetsUpload, () => initialState)
);
