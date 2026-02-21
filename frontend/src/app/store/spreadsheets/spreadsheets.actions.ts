import { createAction, props } from '@ngrx/store';

export interface FileResultSlot {
  filename: string;
  loading: boolean;
  error?: string;
  data?: any[];
  columns?: string[];
}

export interface SpreadsheetsResponse {
  filename: string;
  total_rows: number;
  columns: string[];
  data: any[];
}

/** Create raw upload slots (loading) before HTTP starts */
export const setRawSlots = createAction(
  '[Spreadsheets] Set Raw Slots',
  props<{ filenames: string[] }>()
);

/** Create cleaned upload slots (loading) before HTTP starts */
export const setCleanedSlots = createAction(
  '[Spreadsheets] Set Cleaned Slots',
  props<{ filenames: string[] }>()
);

export const rawFileSuccess = createAction(
  '[Spreadsheets] Raw File Success',
  props<{ index: number; result: SpreadsheetsResponse }>()
);

export const rawFileError = createAction(
  '[Spreadsheets] Raw File Error',
  props<{ index: number; error: string }>()
);

export const cleanedFileSuccess = createAction(
  '[Spreadsheets] Cleaned File Success',
  props<{ index: number; result: SpreadsheetsResponse }>()
);

export const cleanedFileError = createAction(
  '[Spreadsheets] Cleaned File Error',
  props<{ index: number; error: string }>()
);

/** Trigger parallel raw uploads (effect will run HTTP); payload is non-serializable */
export const uploadRawFiles = createAction(
  '[Spreadsheets] Upload Raw Files',
  props<{ files: File[] }>()
);

/** Trigger parallel cleaned uploads when user clicks Cleaned Data tab */
export const uploadCleanedFiles = createAction(
  '[Spreadsheets] Upload Cleaned Files',
  props<{ files: File[] }>()
);

export const setSelectedRawIndex = createAction(
  '[Spreadsheets] Set Selected Raw Index',
  props<{ index: number }>()
);

export const setSelectedCleanedIndex = createAction(
  '[Spreadsheets] Set Selected Cleaned Index',
  props<{ index: number }>()
);

export const resetSpreadsheetsUpload = createAction(
  '[Spreadsheets] Reset Upload'
);
