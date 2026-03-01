import { Routes } from '@angular/router';
import { MainLayoutComponent } from './layout/main-layout.component';
import { DashboardPage } from './dashboard-page/dashboard-page';
import { SpreadsheetsPage } from './spreadsheets-page/spreadsheets-page';

export const routes: Routes = [
  {
    path: '',
    component: MainLayoutComponent,
    children: [
      {
        path: 'dashboard',
        component: DashboardPage,
      },
      {
        path: 'spreadsheets',
        component: SpreadsheetsPage,
      },
      {
        path: '',
        redirectTo: 'dashboard',
        pathMatch: 'full',
      },
    ],
  },
];