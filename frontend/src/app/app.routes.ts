import { Routes } from '@angular/router';
import { DashboardPage } from './dashboard-page/dashboard-page';
import { AnalyzePage } from './analyze-page/analyze-page';

export const routes: Routes = [
    {
        path: 'dashboard',
        component: DashboardPage
    },
    {
        path: 'analyze',
        component: AnalyzePage
    },
    {
        path: '',
        redirectTo: 'dashboard',
        pathMatch: 'full'
    }
];