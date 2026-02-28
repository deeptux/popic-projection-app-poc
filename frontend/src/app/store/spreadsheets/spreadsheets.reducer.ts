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
  resetSpreadsheetsUpload,
  setCommissionRawSlots,
  setCommissionCleanedSlots,
  commissionRawFileSuccess,
  commissionRawFileError,
  commissionCleanedFileSuccess,
  commissionCleanedFileError,
  setSelectedCommissionRawIndex,
  setSelectedCommissionCleanedIndex,
  setReferralRawSlots,
  setReferralCleanedSlots,
  referralRawFileSuccess,
  referralRawFileError,
  referralCleanedFileSuccess,
  referralCleanedFileError,
  setSelectedReferralRawIndex,
  setSelectedReferralCleanedIndex,
} from './spreadsheets.actions';

export interface SpreadsheetsState {
  rawFileResults: FileResultSlot[];
  cleanedFileResults: FileResultSlot[];
  selectedRawIndex: number;
  selectedCleanedIndex: number;
  commissionRawFileResults: FileResultSlot[];
  commissionCleanedFileResults: FileResultSlot[];
  selectedCommissionRawIndex: number;
  selectedCommissionCleanedIndex: number;
  referralRawFileResults: FileResultSlot[];
  referralCleanedFileResults: FileResultSlot[];
  selectedReferralRawIndex: number;
  selectedReferralCleanedIndex: number;
}

const initialState: SpreadsheetsState = {
  rawFileResults: [],
  cleanedFileResults: [],
  selectedRawIndex: 0,
  selectedCleanedIndex: 0,
  commissionRawFileResults: [],
  commissionCleanedFileResults: [],
  selectedCommissionRawIndex: 0,
  selectedCommissionCleanedIndex: 0,
  referralRawFileResults: [],
  referralCleanedFileResults: [],
  selectedReferralRawIndex: 0,
  selectedReferralCleanedIndex: 0,
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
  on(resetSpreadsheetsUpload, () => initialState),
  on(setCommissionRawSlots, (state, { filenames }) => ({
    ...state,
    commissionRawFileResults: filenames.map(filename => ({ filename, loading: true })),
    selectedCommissionRawIndex: 0,
  })),
  on(commissionRawFileSuccess, (state, { index, result }) => {
    const next = [...state.commissionRawFileResults];
    if (next[index]) {
      next[index] = {
        filename: result.filename,
        loading: false,
        data: result.data,
        columns: result.columns,
      };
    }
    return { ...state, commissionRawFileResults: next };
  }),
  on(commissionRawFileError, (state, { index, error }) => {
    const next = [...state.commissionRawFileResults];
    if (next[index]) {
      next[index] = { ...next[index], loading: false, error: error || 'Upload failed' };
    }
    return { ...state, commissionRawFileResults: next };
  }),
  on(setCommissionCleanedSlots, (state, { filenames }) => ({
    ...state,
    commissionCleanedFileResults: filenames.map(filename => ({ filename, loading: true })),
    selectedCommissionCleanedIndex: 0,
  })),
  on(commissionCleanedFileSuccess, (state, { index, result }) => {
    const next = [...state.commissionCleanedFileResults];
    if (next[index]) {
      next[index] = {
        filename: result.filename,
        loading: false,
        data: result.data,
        columns: result.columns,
      };
    }
    return { ...state, commissionCleanedFileResults: next };
  }),
  on(commissionCleanedFileError, (state, { index, error }) => {
    const next = [...state.commissionCleanedFileResults];
    if (next[index]) {
      next[index] = { ...next[index], loading: false, error: error || 'Upload failed' };
    }
    return { ...state, commissionCleanedFileResults: next };
  }),
  on(setSelectedCommissionRawIndex, (state, { index }) => ({
    ...state,
    selectedCommissionRawIndex: index,
  })),
  on(setSelectedCommissionCleanedIndex, (state, { index }) => ({
    ...state,
    selectedCommissionCleanedIndex: index,
  })),
  on(setReferralRawSlots, (state, { filenames }) => ({
    ...state,
    referralRawFileResults: filenames.map(filename => ({ filename, loading: true })),
    selectedReferralRawIndex: 0,
  })),
  on(referralRawFileSuccess, (state, { index, result }) => {
    const next = [...state.referralRawFileResults];
    if (next[index]) {
      next[index] = {
        filename: result.filename,
        loading: false,
        data: result.data,
        columns: result.columns,
      };
    }
    return { ...state, referralRawFileResults: next };
  }),
  on(referralRawFileError, (state, { index, error }) => {
    const next = [...state.referralRawFileResults];
    if (next[index]) {
      next[index] = { ...next[index], loading: false, error: error || 'Upload failed' };
    }
    return { ...state, referralRawFileResults: next };
  }),
  on(setReferralCleanedSlots, (state, { filenames }) => ({
    ...state,
    referralCleanedFileResults: filenames.map(filename => ({ filename, loading: true })),
    selectedReferralCleanedIndex: 0,
  })),
  on(referralCleanedFileSuccess, (state, { index, result }) => {
    const next = [...state.referralCleanedFileResults];
    if (next[index]) {
      next[index] = {
        filename: result.filename,
        loading: false,
        data: result.data,
        columns: result.columns,
      };
    }
    return { ...state, referralCleanedFileResults: next };
  }),
  on(referralCleanedFileError, (state, { index, error }) => {
    const next = [...state.referralCleanedFileResults];
    if (next[index]) {
      next[index] = { ...next[index], loading: false, error: error || 'Upload failed' };
    }
    return { ...state, referralCleanedFileResults: next };
  }),
  on(setSelectedReferralRawIndex, (state, { index }) => ({
    ...state,
    selectedReferralRawIndex: index,
  })),
  on(setSelectedReferralCleanedIndex, (state, { index }) => ({
    ...state,
    selectedReferralCleanedIndex: index,
  })),
);
