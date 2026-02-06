import { Component } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { CommonModule } from '@angular/common';
import { MatTableModule } from '@angular/material/table';
import { RouterModule } from '@angular/router'; // Added for sidebar navigation support

interface AnalyzeResponse {
  filename: string;
  total_rows: number;
  columns: string[];
  data: any[];
}

@Component({
  selector: 'app-analyze-page',
  standalone: true,
  imports: [CommonModule, MatTableModule, RouterModule],
  templateUrl: './analyze-page.html',
  styleUrl: './analyze-page.css'
})
export class AnalyzePage {
  // --- Tab Logic ---
  activeTab: 'salesforce' | 'commissions' | 'referral' = 'salesforce';

  salesforceSubTab: 'raw' | 'consolidated' | 'metrics' = 'raw';

  selectedFile: File | null = null;

  // --- Data for Tab 1 (Salesforce) ---
  dataSource: any[] = [];
  displayedColumns: string[] = [];

  cleanedSalesforceCaptiveSummaryDataSource: any[] = [];
  cleanedSalesforceCaptiveSummaryColumns: any[] = [];

  // --- Data for Tab 2 (Financial) ---
  commissionsDataSource: any[] = [];
  commissionsColumns: string[] = [];

  // --- Data for Tab 3 (Financial) ---
  referralDataSource: any[] = [];
  referralColumns: string[] = [];

  constructor(private http: HttpClient) { }

  // Set which tab is active
  switchTab(tab: 'salesforce' | 'commissions' | 'referral') {
    this.activeTab = tab;
    this.selectedFile = null; // Reset file selection when switching tabs
  }

  onFileSelected(event: any) {
    const file: File = event.target.files[0];
    if (file) {
      this.selectedFile = file;
    }
  }

  uploadFile() {
    if (!this.selectedFile) return;

    const formData = new FormData();
    formData.append('file', this.selectedFile);
    formData.append('active_tab', this.activeTab);

    // alert(this.activeTab);

    // 1. Define Endpoints
    const endpoint = 'http://127.0.0.1:8000/upload/salesforce-captive-summary/basic';

    // 2. First Request (Raw Data)
    this.http.post<AnalyzeResponse>(endpoint, formData)
      .subscribe({
        next: (response) => {

          // --- Handle Salesforce Logic ---
          if (this.activeTab === 'salesforce') {
            // A. Store the Raw Data
            this.dataSource = response.data;
            this.displayedColumns = response.columns;

            const cleanedEndpoint = 'http://127.0.0.1:8000/analyze'; // Assumption: this is your new python endpoint

            // B. TRIGGER SECOND REQUEST (Nested Subscription)
            // We reuse the same formData since it's the same file
            this.http.post<AnalyzeResponse>(cleanedEndpoint, formData)
              .subscribe({
                next: (consResponse) => {
                  console.log('Consolidated analysis complete');
                  // You need to define these variables in your class:
                  this.cleanedSalesforceCaptiveSummaryDataSource = consResponse.data;
                  // If the columns are different, use a separate variable, or reuse displayedColumns if identical
                  this.cleanedSalesforceCaptiveSummaryColumns = consResponse.columns;
                },
                error: (err) => console.error('Consolidated upload failed:', err)
              });
          }
          // --- Handle Other Tabs ---
          else if (this.activeTab === 'commissions') {
            this.commissionsDataSource = response.data;
            this.commissionsColumns = response.columns;
          }
          else {
            this.referralDataSource = response.data;
            this.referralColumns = response.columns;
          }

          this.selectedFile = null;
        },
        error: (err) => {
          console.error('Upload error:', err);
          alert('Upload failed. Check if the Python backend is running at ' + endpoint);
        }
      });

  }
}