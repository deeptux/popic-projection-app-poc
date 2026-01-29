import { Component } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { CommonModule } from '@angular/common';
import { MatTableModule } from '@angular/material/table'; // Import Table

// 1. Define the model to match your Python response
interface AnalyzeResponse {
  filename: string;
  total_rows: number;
  columns: string[];
  data: any[]; // You can use a more specific type later if needed
}

@Component({
  selector: 'app-analyze-page',
  standalone: true,
  imports: [CommonModule, MatTableModule],
  templateUrl: './analyze-page.html',
  styleUrl: './analyze-page.css'
})
export class AnalyzePage {
  selectedFile: File | null = null;

  // Data variables
  dataSource: any[] = [];
  displayedColumns: string[] = [];

  constructor(private http: HttpClient) { }

  onFileSelected(event: any) {
    const file: File = event.target.files[0];
    if (file) {
      this.selectedFile = file;
      console.log('Selected file:', file.name);
    }
  }

  uploadFile() {
    if (!this.selectedFile) return;

    const formData = new FormData();
    formData.append('file', this.selectedFile);

    // This URL must match your FastAPI address
    this.http.post<AnalyzeResponse>('http://127.0.0.1:8000/analyze', formData)
      .subscribe({
        next: (response) => {
          console.log('Analysis Results:', response);
          alert('Upload successful! Check console for data.');
          this.dataSource = response.data;
          this.displayedColumns = response.columns;
          console.log('Table Data Loaded');
        },
        error: (err) => {
          console.error('Upload error:', err);
          alert('Upload failed. Is the Python backend running?');
        }
      });
  }
}