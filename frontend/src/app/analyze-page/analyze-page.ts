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

  selectedFile: File | null = null;

  // --- Data for Tab 1 (Salesforce) ---
  dataSource: any[] = [];
  displayedColumns: string[] = [];

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

    // We can use the same endpoint or different ones based on activeTab
    const endpoint = this.activeTab === 'salesforce'
      ? 'http://127.0.0.1:8000/analyze'
      : 'http://127.0.0.1:8000/analyze'; // Example second endpoint

    this.http.post<AnalyzeResponse>(endpoint, formData)
      .subscribe({
        next: (response) => {
          if (this.activeTab === 'salesforce') {
            this.dataSource = response.data;
            this.displayedColumns = response.columns;
          }
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