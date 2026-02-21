import { inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { catchError, merge, mergeMap, of } from 'rxjs';
import { map } from 'rxjs/operators';
import {
  uploadRawFiles,
  uploadCleanedFiles,
  rawFileSuccess,
  rawFileError,
  cleanedFileSuccess,
  cleanedFileError,
  setRawSlots,
  setCleanedSlots,
  SpreadsheetsResponse
} from './spreadsheets.actions';
import { Store } from '@ngrx/store';
import type { SpreadsheetsState } from './spreadsheets.reducer';

const API_BASE = 'http://127.0.0.1:8000';
const RAW_ENDPOINT = `${API_BASE}/upload/salesforce-captive-summary/basic`;
const CLEANED_ENDPOINT = `${API_BASE}/upload/salesforce-captive-summary`;

export class SpreadsheetsEffects {
  private readonly actions$ = inject(Actions);
  private readonly http = inject(HttpClient);
  private readonly store = inject(Store<{ spreadsheets: SpreadsheetsState }>);

  uploadRawFiles$ = createEffect(() =>
    this.actions$.pipe(
      ofType(uploadRawFiles),
      mergeMap(({ files }) => {
        const filenames = files.map(f => f.name);
        this.store.dispatch(setRawSlots({ filenames }));
        const observables = files.map((file, index) => {
          const formData = new FormData();
          formData.append('file', file);
          return this.http.post<SpreadsheetsResponse>(RAW_ENDPOINT, formData).pipe(
            map(result => rawFileSuccess({ index, result })),
            catchError(err =>
              of(
                rawFileError({
                  index,
                  error: err?.message || err?.error?.detail || 'Request failed'
                })
              )
            )
          );
        });
        return observables.length > 0 ? merge(...observables) : of();
      })
    )
  );

  uploadCleanedFiles$ = createEffect(() =>
    this.actions$.pipe(
      ofType(uploadCleanedFiles),
      mergeMap(({ files }) => {
        const filenames = files.map(f => f.name);
        this.store.dispatch(setCleanedSlots({ filenames }));
        const observables = files.map((file, index) => {
          const formData = new FormData();
          formData.append('file', file);
          formData.append('active_tab', 'salesforce');
          return this.http.post<SpreadsheetsResponse>(CLEANED_ENDPOINT, formData).pipe(
            map(result => cleanedFileSuccess({ index, result })),
            catchError(err =>
              of(
                cleanedFileError({
                  index,
                  error: err?.message || err?.error?.detail || 'Request failed'
                })
              )
            )
          );
        });
        return observables.length > 0 ? merge(...observables) : of();
      })
    )
  );
}
