import { Component } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-analyze-page',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './analyze-page.html',
  styleUrl: './analyze-page.css'
})
export class AnalyzePageComponent {
  selectedFile: File | null = null;

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
    this.http.post('http://127.0.0.1:8000/analyze', formData)
      .subscribe({
        next: (response) => {
          console.log('Analysis Results:', response);
          alert('Upload successful! Check console for data.');
        },
        error: (err) => {
          console.error('Upload error:', err);
          alert('Upload failed. Is the Python backend running?');
        }
      });
  }
}