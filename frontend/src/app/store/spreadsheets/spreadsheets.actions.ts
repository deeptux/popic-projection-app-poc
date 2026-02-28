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

// --- Commission-specific (same shape as above, separate state) ---
export const setCommissionRawSlots = createAction(
  '[Spreadsheets] Set Commission Raw Slots',
  props<{ filenames: string[] }>()
);
export const setCommissionCleanedSlots = createAction(
  '[Spreadsheets] Set Commission Cleaned Slots',
  props<{ filenames: string[] }>()
);
export const commissionRawFileSuccess = createAction(
  '[Spreadsheets] Commission Raw File Success',
  props<{ index: number; result: SpreadsheetsResponse }>()
);
export const commissionRawFileError = createAction(
  '[Spreadsheets] Commission Raw File Error',
  props<{ index: number; error: string }>()
);
export const commissionCleanedFileSuccess = createAction(
  '[Spreadsheets] Commission Cleaned File Success',
  props<{ index: number; result: SpreadsheetsResponse }>()
);
export const commissionCleanedFileError = createAction(
  '[Spreadsheets] Commission Cleaned File Error',
  props<{ index: number; error: string }>()
);
export const uploadCommissionRawFiles = createAction(
  '[Spreadsheets] Upload Commission Raw Files',
  props<{ files: File[] }>()
);
export const uploadCommissionCleanedFiles = createAction(
  '[Spreadsheets] Upload Commission Cleaned Files',
  props<{ files: File[] }>()
);
export const setSelectedCommissionRawIndex = createAction(
  '[Spreadsheets] Set Selected Commission Raw Index',
  props<{ index: number }>()
);
export const setSelectedCommissionCleanedIndex = createAction(
  '[Spreadsheets] Set Selected Commission Cleaned Index',
  props<{ index: number }>()
);

// --- Referral-specific (same shape as Commission, separate state) ---
export const setReferralRawSlots = createAction(
  '[Spreadsheets] Set Referral Raw Slots',
  props<{ filenames: string[] }>()
);
export const setReferralCleanedSlots = createAction(
  '[Spreadsheets] Set Referral Cleaned Slots',
  props<{ filenames: string[] }>()
);
export const referralRawFileSuccess = createAction(
  '[Spreadsheets] Referral Raw File Success',
  props<{ index: number; result: SpreadsheetsResponse }>()
);
export const referralRawFileError = createAction(
  '[Spreadsheets] Referral Raw File Error',
  props<{ index: number; error: string }>()
);
export const referralCleanedFileSuccess = createAction(
  '[Spreadsheets] Referral Cleaned File Success',
  props<{ index: number; result: SpreadsheetsResponse }>()
);
export const referralCleanedFileError = createAction(
  '[Spreadsheets] Referral Cleaned File Error',
  props<{ index: number; error: string }>()
);
export const uploadReferralRawFiles = createAction(
  '[Spreadsheets] Upload Referral Raw Files',
  props<{ files: File[] }>()
);
export const uploadReferralCleanedFiles = createAction(
  '[Spreadsheets] Upload Referral Cleaned Files',
  props<{ files: File[] }>()
);
export const setSelectedReferralRawIndex = createAction(
  '[Spreadsheets] Set Selected Referral Raw Index',
  props<{ index: number }>()
);
export const setSelectedReferralCleanedIndex = createAction(
  '[Spreadsheets] Set Selected Referral Cleaned Index',
  props<{ index: number }>()
);
