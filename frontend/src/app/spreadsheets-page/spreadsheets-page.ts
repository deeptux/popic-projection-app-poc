import {
  Component,
  inject,
  signal,
  computed,
  OnInit,
  OnDestroy,
  ViewChild,
  ElementRef
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { MatTableModule } from '@angular/material/table';
import { RouterModule } from '@angular/router';
import { Store } from '@ngrx/store';
import { toSignal } from '@angular/core/rxjs-interop';

import {
  selectRawFileResults,
  selectCleanedFileResults,
  selectSelectedRawIndex,
  selectSelectedCleanedIndex,
  selectHasAnyRawResult,
  uploadRawFiles,
  uploadCleanedFiles,
  setSelectedRawIndex,
  setSelectedCleanedIndex,
  resetSpreadsheetsUpload
} from '../store/spreadsheets';

const API_BASIC = 'http://127.0.0.1:8000/upload/salesforce-captive-summary/basic';

interface SpreadsheetsResponse {
  filename: string;
  total_rows: number;
  columns: string[];
  data: any[];
}

const MAX_FILES = 3;
const ALLOWED_EXTENSIONS = ['.xlsx', '.xls', '.csv'];

function getAllowedExtensionsSet(): Set<string> {
  const set = new Set<string>();
  ALLOWED_EXTENSIONS.forEach(ext => set.add(ext.toLowerCase()));
  return set;
}

const ALLOWED = getAllowedExtensionsSet();

function isAllowedFile(file: File): boolean {
  const name = file.name.toLowerCase();
  return ALLOWED_EXTENSIONS.some(ext => name.endsWith(ext.toLowerCase()));
}

function validateFiles(files: File[]): { valid: File[]; error: string | null } {
  if (files.length > MAX_FILES) {
    return {
      valid: [],
      error: `Maximum ${MAX_FILES} files allowed. Please select up to ${MAX_FILES} spreadsheets.`
    };
  }
  const invalid = files.filter(f => !isAllowedFile(f));
  if (invalid.length > 0) {
    return {
      valid: [],
      error: 'Only .xlsx, .xls, and .csv files are allowed.'
    };
  }
  return { valid: files, error: null };
}

/** Truncate filename to maxLen with ellipsis in the middle; full name for tooltip. */
export function truncateFilename(name: string, maxLen = 40): string {
  if (!name || name.length <= maxLen) return name;
  const half = Math.floor((maxLen - 3) / 2);
  return name.slice(0, half) + '...' + name.slice(-half);
}

@Component({
  selector: 'app-spreadsheets-page',
  standalone: true,
  imports: [CommonModule, MatTableModule, RouterModule],
  templateUrl: './spreadsheets-page.html',
  styleUrl: './spreadsheets-page.css'
})
export class SpreadsheetsPage implements OnInit, OnDestroy {
  private readonly store = inject(Store);
  private readonly http = inject(HttpClient);

  @ViewChild('fileInput') fileInputRef?: ElementRef<HTMLInputElement>;

  activeTab: 'salesforce' | 'commissions' | 'referral' = 'salesforce';
  salesforceSubTab: 'raw' | 'consolidated' | 'metrics' = 'raw';

  readonly selectedFiles = signal<File[]>([]);
  readonly fileTypeError = signal<string | null>(null);
  private filesForCleanedRequest: File[] = [];

  selectedFile: File | null = null;
  commissionsDataSource: any[] = [];
  commissionsColumns: string[] = [];
  referralDataSource: any[] = [];
  referralColumns: string[] = [];

  readonly rawFileResults = toSignal(this.store.select(selectRawFileResults), { initialValue: [] });
  readonly cleanedFileResults = toSignal(this.store.select(selectCleanedFileResults), { initialValue: [] });
  readonly selectedRawIndex = toSignal(this.store.select(selectSelectedRawIndex), { initialValue: 0 });
  readonly selectedCleanedIndex = toSignal(this.store.select(selectSelectedCleanedIndex), { initialValue: 0 });
  readonly hasAnyRawResult = toSignal(this.store.select(selectHasAnyRawResult), { initialValue: false });

  readonly canUpload = computed(() => this.selectedFiles().length > 0);
  readonly rawSlotCount = computed(() => this.rawFileResults().length);
  readonly cleanedSlotCount = computed(() => this.cleanedFileResults().length);
  readonly hasRawSubTabs = computed(() => this.rawSlotCount() >= 2);
  readonly hasCleanedSubTabs = computed(() => this.cleanedSlotCount() >= 2);

  readonly currentRawSlot = computed(() => {
    const slots = this.rawFileResults();
    const idx = this.selectedRawIndex();
    return slots[idx] ?? null;
  });

  readonly currentCleanedSlot = computed(() => {
    const slots = this.cleanedFileResults();
    const idx = this.selectedCleanedIndex();
    return slots[idx] ?? null;
  });

  ngOnInit(): void {
    // Optional: one-time init if needed
  }

  ngOnDestroy(): void {
    this.store.dispatch(resetSpreadsheetsUpload());
  }

  switchTab(tab: 'salesforce' | 'commissions' | 'referral'): void {
    this.activeTab = tab;
    this.selectedFiles.set([]);
    this.selectedFile = null;
    this.fileTypeError.set(null);
  }

  onFileSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    if (file) this.selectedFile = file;
  }

  onFilesChosen(files: FileList | null): void {
    this.fileTypeError.set(null);
    if (!files || files.length === 0) return;
    const list = Array.from(files);
    const { valid, error } = validateFiles(list);
    if (error) {
      this.fileTypeError.set(error);
      this.selectedFiles.set([]);
      return;
    }
    const trimmed = valid.slice(0, MAX_FILES);
    this.selectedFiles.set(trimmed);
  }

  onFileInputChange(event: Event): void {
    const input = event.target as HTMLInputElement;
    this.onFilesChosen(input.files);
    input.value = '';
  }

  onDrop(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
    const files = event.dataTransfer?.files;
    this.onFilesChosen(files ?? null);
  }

  onDragOver(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
  }

  onDragLeave(event: DragEvent): void {
    event.preventDefault();
    event.stopPropagation();
  }

  triggerFileInput(): void {
    this.fileInputRef?.nativeElement?.click();
  }

  uploadFile(): void {
    if (this.activeTab === 'salesforce') {
      const files = this.selectedFiles();
      if (files.length === 0) return;
      const { valid, error } = validateFiles(files);
      if (error) {
        this.fileTypeError.set(error);
        return;
      }
      this.fileTypeError.set(null);
      this.filesForCleanedRequest = [...valid];
      this.store.dispatch(resetSpreadsheetsUpload());
      this.store.dispatch(setSelectedRawIndex({ index: 0 }));
      this.store.dispatch(setSelectedCleanedIndex({ index: 0 }));
      this.salesforceSubTab = 'raw';
      this.store.dispatch(uploadRawFiles({ files: valid }));
      this.selectedFiles.set([]);
      return;
    }
    if (this.activeTab === 'commissions' || this.activeTab === 'referral') {
      if (!this.selectedFile) return;
      const formData = new FormData();
      formData.append('file', this.selectedFile);
      formData.append('active_tab', this.activeTab);
      this.http.post<SpreadsheetsResponse>(API_BASIC, formData).subscribe({
        next: (res) => {
          if (this.activeTab === 'commissions') {
            this.commissionsDataSource = res.data;
            this.commissionsColumns = res.columns;
          } else {
            this.referralDataSource = res.data;
            this.referralColumns = res.columns;
          }
          this.selectedFile = null;
        },
        error: (err) => {
          console.error('Upload error:', err);
          alert('Upload failed. Check if the backend is running at ' + API_BASIC);
        }
      });
    }
  }

  setRawSubTab(index: number): void {
    this.store.dispatch(setSelectedRawIndex({ index }));
  }

  setCleanedSubTab(index: number): void {
    this.store.dispatch(setSelectedCleanedIndex({ index }));
  }

  onRawDataTabClick(): void {
    this.salesforceSubTab = 'raw';
  }

  onCleanedDataTabClick(): void {
    this.salesforceSubTab = 'consolidated';
    if (this.filesForCleanedRequest.length > 0 && this.cleanedFileResults().length === 0) {
      this.store.dispatch(uploadCleanedFiles({ files: this.filesForCleanedRequest }));
    }
  }

  onMetricsTabClick(): void {
    this.salesforceSubTab = 'metrics';
  }

  removeFile(index: number): void {
    const current = this.selectedFiles();
    const next = current.filter((_, i) => i !== index);
    this.selectedFiles.set(next);
    if (next.length === 0) this.fileTypeError.set(null);
  }

  clearAllFiles(): void {
    this.selectedFiles.set([]);
    this.fileTypeError.set(null);
  }

  truncateFilename = truncateFilename;
}
