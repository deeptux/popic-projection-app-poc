import { ApplicationConfig, provideBrowserGlobalErrorListeners, provideZoneChangeDetection } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient } from '@angular/common/http';
import { provideStore } from '@ngrx/store';
import { provideEffects } from '@ngrx/effects';

import { routes } from './app.routes';
import { spreadsheetsReducer } from './store/spreadsheets/spreadsheets.reducer';
import { SpreadsheetsEffects } from './store/spreadsheets/spreadsheets.effects';

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideZoneChangeDetection({ eventCoalescing: true }),
    provideRouter(routes),
    provideHttpClient(),
    provideStore({ spreadsheets: spreadsheetsReducer }),
    provideEffects(SpreadsheetsEffects)
  ]
};
