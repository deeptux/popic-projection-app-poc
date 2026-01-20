import { Routes } from '@angular/router';
import { AnalyzePageComponent } from './analyze-page/analyze-page'; // No .component, just the filename

export const routes: Routes = [
    {
        path: 'analyze',
        component: AnalyzePageComponent
    },
    {
        path: '',
        redirectTo: 'analyze',
        pathMatch: 'full'
    }
];