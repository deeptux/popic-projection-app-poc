import {
  Component,
  inject,
  signal,
  computed,
  OnInit,
  OnDestroy,
  ViewChild,
  ElementRef,
  effect
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { MatTableModule } from '@angular/material/table';
import { RouterModule } from '@angular/router';
import { Store } from '@ngrx/store';
import { toSignal } from '@angular/core/rxjs-interop';
import { BaseChartDirective } from 'ng2-charts';
import { forkJoin } from 'rxjs';

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
const API_ANALYTICS = 'http://127.0.0.1:8000/analytics';

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

/** Truncate legend/category label to maxLen with ellipsis in the middle (for chart axes). */
export function truncateLegendLabel(name: string, maxLen = 20): string {
  if (!name || name.length <= maxLen) return name;
  const half = Math.floor((maxLen - 3) / 2);
  return name.slice(0, half) + '…' + name.slice(-half);
}

/** Fisher–Yates shuffle (mutates array). */
function shuffleArray<T>(arr: T[]): void {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
}

/** Format numeric values with commas and 2 decimal places; pass-through non-numbers. */
export function formatCellValue(val: unknown): string {
  if (val === null || val === undefined) return '';
  const n = Number(val);
  if (!Number.isNaN(n)) {
    return n.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 0 });
  }
  return String(val);
}

/** Heuristic: column is numeric if first non-null values look like numbers. */
function isColumnNumeric(data: any[], columnKey: string): boolean {
  for (const row of data) {
    const v = row[columnKey];
    if (v === null || v === undefined || v === '') continue;
    const n = Number(v);
    if (Number.isNaN(n)) return false;
  }
  return true;
}

export interface AnalyticsSlot {
  data: any[];
  columns: string[];
}

/** Cached analytics per file index (used to avoid repeat API requests when switching Checked Stats sub-tabs). */
interface AnalyticsCacheEntry {
  line: { labels: string[]; values: number[] } | null;
  bar: { labels: string[]; values: number[] } | null;
  pieRlip: { label: string; value: number; percent: number }[];
  pieRap: { label: string; value: number; percent: number }[];
  pieCompare: { label: string; value: number; percent: number }[];
}

@Component({
  selector: 'app-spreadsheets-page',
  standalone: true,
  imports: [CommonModule, MatTableModule, RouterModule, BaseChartDirective],
  templateUrl: './spreadsheets-page.html',
  styleUrl: './spreadsheets-page.css'
})
export class SpreadsheetsPage implements OnInit, OnDestroy {
  private readonly store = inject(Store);
  private readonly http = inject(HttpClient);

  @ViewChild('fileInput') fileInputRef?: ElementRef<HTMLInputElement>;

  activeTab: 'salesforce' | 'commissions' | 'referral' = 'salesforce';
  salesforceSubTab: 'raw' | 'cleanedData' | 'checkedStats' = 'raw';

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

  readonly checkedStatsTabActive = signal(false);
  readonly checkedStatsLoading = signal(false);
  readonly checkedStatsError = signal<string | null>(null);
  readonly lineChartData = signal<{ labels: string[]; values: number[] } | null>(null);
  readonly barChartData = signal<{ labels: string[]; values: number[] } | null>(null);
  readonly pieRlipData = signal<{ label: string; value: number; percent: number }[] | null>(null);
  readonly pieRapData = signal<{ label: string; value: number; percent: number }[] | null>(null);
  readonly pieCompareData = signal<{ label: string; value: number; percent: number }[] | null>(null);

  /** Per-file analytics cache (key = selectedCleanedIndex). Cleared on new upload. */
  private readonly analyticsCache = new Map<number, AnalyticsCacheEntry>();

  readonly cleanedSearchTerms = signal<string[]>([]);
  readonly cleanedSortState = signal<Array<{ columnKey: string | null; direction: 'asc' | 'desc' }>>([]);
  readonly cleanedPageIndex = signal<number[]>([]);
  readonly cleanedPageSize = signal<number[]>([]);

  readonly CLEANED_PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

  constructor() {
    effect(() => {
      if (!this.checkedStatsTabActive()) return;
      const idx = this.selectedCleanedIndex();
      const slot = this.currentCleanedSlot();
      if (!slot?.data?.length || !slot?.columns?.length) {
        this.lineChartData.set(null);
        this.barChartData.set(null);
        this.pieRlipData.set(null);
        this.pieRapData.set(null);
        this.pieCompareData.set(null);
        this.checkedStatsError.set(null);
        return;
      }
      const cached = this.analyticsCache.get(idx);
      if (cached) {
        this.applyCachedAnalytics(cached);
        this.checkedStatsLoading.set(false);
        this.checkedStatsError.set(null);
        return;
      }
      this.loadCheckedStatsCharts({ data: slot.data, columns: slot.columns }, idx);
    });
  }

  private applyCachedAnalytics(cached: AnalyticsCacheEntry): void {
    this.lineChartData.set(cached.line);
    this.barChartData.set(cached.bar);
    this.pieRlipData.set(cached.pieRlip);
    this.pieRapData.set(cached.pieRap);
    this.pieCompareData.set(cached.pieCompare);
  }

  private clearAnalyticsCacheAndCharts(): void {
    this.analyticsCache.clear();
    this.lineChartData.set(null);
    this.barChartData.set(null);
    this.pieRlipData.set(null);
    this.pieRapData.set(null);
    this.pieCompareData.set(null);
    this.checkedStatsError.set(null);
    this.checkedStatsLoading.set(false);
  }

  ngOnInit(): void {}

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
      this.clearAnalyticsCacheAndCharts();
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
    this.checkedStatsTabActive.set(false);
  }

  onCleanedDataTabClick(): void {
    this.salesforceSubTab = 'cleanedData';
    this.checkedStatsTabActive.set(false);
    if (this.filesForCleanedRequest.length > 0 && this.cleanedFileResults().length === 0) {
      this.store.dispatch(uploadCleanedFiles({ files: this.filesForCleanedRequest }));
    }
  }

  onCheckedStatsTabClick(): void {
    this.salesforceSubTab = 'checkedStats';
    this.checkedStatsTabActive.set(true);
    if (this.filesForCleanedRequest.length > 0 && this.cleanedFileResults().length === 0) {
      this.store.dispatch(uploadCleanedFiles({ files: this.filesForCleanedRequest }));
    }
  }

  setCleanedSubTabForCheckedStats(index: number): void {
    this.store.dispatch(setSelectedCleanedIndex({ index }));
  }

  loadCheckedStatsCharts(slot: AnalyticsSlot, fileIndex: number): void {
    this.checkedStatsError.set(null);
    this.checkedStatsLoading.set(true);
    const body = { data: slot.data, columns: slot.columns };
    forkJoin({
      line: this.http.post<{ labels: string[]; values: number[] }>(`${API_ANALYTICS}/top-additional-rent-line`, body),
      bar: this.http.post<{ labels: string[]; values: number[] }>(`${API_ANALYTICS}/top-total-available-units-bar`, body),
      pieRlip: this.http.post<{ slices: { label: string; value: number; percent: number }[] }>(`${API_ANALYTICS}/pie-popic-fee-rlip`, body),
      pieRap: this.http.post<{ slices: { label: string; value: number; percent: number }[] }>(`${API_ANALYTICS}/pie-popic-fee-rap`, body),
      pieCompare: this.http.post<{ slices: { label: string; value: number; percent: number }[] }>(`${API_ANALYTICS}/pie-popic-fee-comparison`, body)
    }).subscribe({
      next: res => {
        let lineEntry: { labels: string[]; values: number[] } | null = res.line;
        if (res.line?.labels?.length && res.line?.values?.length) {
          const pairs = res.line.labels.map((label, i) => ({ label, value: res.line!.values[i] }));
          shuffleArray(pairs);
          lineEntry = { labels: pairs.map(p => p.label), values: pairs.map(p => p.value) };
        }
        this.lineChartData.set(lineEntry);
        this.barChartData.set(res.bar);
        this.pieRlipData.set(res.pieRlip.slices);
        this.pieRapData.set(res.pieRap.slices);
        this.pieCompareData.set(res.pieCompare.slices);
        this.analyticsCache.set(fileIndex, {
          line: lineEntry,
          bar: res.bar,
          pieRlip: res.pieRlip.slices,
          pieRap: res.pieRap.slices,
          pieCompare: res.pieCompare.slices
        });
        this.checkedStatsLoading.set(false);
      },
      error: err => {
        this.checkedStatsError.set(err?.error?.detail || err?.message || 'Analytics request failed');
        this.checkedStatsLoading.set(false);
      }
    });
  }

  getLineChartChartData(): { labels: string[]; datasets: { data: number[]; label: string; [key: string]: unknown }[] } | null {
    const d = this.lineChartData();
    if (!d?.labels?.length) return null;
    return {
      labels: d.labels.map(l => truncateLegendLabel(l, 20)),
      datasets: [{
        data: d.values,
        label: 'Additional Rent',
        fill: true,
        backgroundColor: 'rgba(255, 126, 95, 0.15)',
        borderColor: '#FF7E5F',
        pointBackgroundColor: '#FF7E5F',
        pointBorderColor: '#fff',
        pointBorderWidth: 1.5,
        pointRadius: 5,
        pointHoverRadius: 7,
        tension: 0.4
      }]
    };
  }

  getBarChartChartData(): { labels: string[]; datasets: { data: number[]; label: string; backgroundColor: string[] }[] } | null {
    const d = this.barChartData();
    if (!d?.labels?.length) return null;
    const colors = this.chartColors;
    const barColors = d.values.map((_, i) => colors[i % colors.length]);
    return {
      labels: d.labels.map(l => truncateLegendLabel(l, 20)),
      datasets: [{
        data: d.values,
        label: 'Total Available Units',
        backgroundColor: barColors
      }]
    };
  }

  formatPieValue(value: number): string {
    return '$' + value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  getPieColors(count: number): string[] {
    const palette = ['#FF7E5F', '#FD3A69', '#f97316', '#ea580c', '#64748b', '#c2410c', '#9a3412'];
    return Array.from({ length: count }, (_, i) => palette[i % palette.length]);
  }

  getPieLegendItems(slices: { label: string; value: number; percent: number }[] | null): { label: string; value: number; percent: number; color: string }[] {
    if (!slices?.length) return [];
    const colors = this.getPieColors(slices.length);
    return slices.map((s, i) => ({ ...s, color: colors[i] }));
  }

  getPieChartData(slices: { label: string; value: number; percent: number }[] | null): { labels: string[]; datasets: { data: number[]; backgroundColor: string[] }[] } | null {
    if (!slices?.length) return null;
    const colors = this.getPieColors(slices.length);
    return {
      labels: slices.map(s => `${s.label} ${this.formatPieValue(s.value)} (${s.percent}%)`),
      datasets: [{
        data: slices.map(s => s.value),
        backgroundColor: colors
      }]
    };
  }

  get lineChartOptions() {
    const fullLabels = this.lineChartData()?.labels ?? [];
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: 'top' as const },
        tooltip: {
          callbacks: {
            title: (items: any[]) => {
              const idx = items[0]?.dataIndex;
              return idx !== undefined && fullLabels[idx] ? fullLabels[idx] : '';
            },
            label: (ctx: any) => {
              const val = ctx.parsed?.y ?? ctx.raw;
              const formatted = typeof val === 'number' ? '$' + val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : String(val);
              return 'Additional Rent: ' + formatted;
            }
          }
        }
      },
      scales: {
        x: { ticks: { maxRotation: 45, minRotation: 35, maxTicksLimit: 10 } },
        y: {
          beginAtZero: true,
          ticks: {
            callback: (value: number | string): string | number =>
              typeof value === 'number' ? '$' + value.toLocaleString(undefined, { maximumFractionDigits: 0 }) : value
          }
        }
      }
    };
  }

  get barChartOptions() {
    const fullLabels = this.barChartData()?.labels ?? [];
    const d = this.barChartData();
    const colors = this.chartColors;
    const barColors = d?.values?.map((_, i) => colors[i % colors.length]) ?? [];
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: true,
          position: 'top' as const,
          labels: {
            boxWidth: 14,
            padding: 12,
            usePointStyle: true
          },
          generateLabels: () =>
            (d?.labels ?? []).map((label, i) => ({
              text: truncateLegendLabel(label, 20),
              fillStyle: barColors[i] ?? colors[i % colors.length],
              strokeStyle: barColors[i] ?? colors[i % colors.length],
              index: i
            }))
        },
        tooltip: {
          callbacks: {
            title: (items: any[]) => {
              const idx = items[0]?.dataIndex;
              return idx !== undefined && fullLabels[idx] ? fullLabels[idx] : '';
            },
            label: (ctx: any) => {
              const val = ctx.parsed?.y ?? ctx.raw;
              const formatted = typeof val === 'number' ? val.toLocaleString(undefined, { maximumFractionDigits: 0 }) : String(val);
              return 'Units: ' + formatted;
            }
          }
        }
      },
      scales: {
        x: { ticks: { maxRotation: 45, minRotation: 35, maxTicksLimit: 8 } },
        y: {
          beginAtZero: true,
          ticks: {
            callback: (value: number | string): string | number =>
              typeof value === 'number' ? value.toLocaleString(undefined, { maximumFractionDigits: 0 }) : value
          }
        }
      }
    };
  }

  pieChartOptions = {
    responsive: true,
    plugins: { legend: { display: false } }
  };
  chartColors = ['#FF7E5F', '#FD3A69', '#f97316', '#ea580c', '#c2410c', '#9a3412', '#7c2d12'];
  pieChartColors = [{ backgroundColor: ['#FF7E5F', '#FD3A69', '#f97316', '#ea580c', '#64748b'] }];

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
  formatCellValue = formatCellValue;

  getCleanedTableData(slot: { data?: any[]; columns?: string[] } | null): any[] {
    if (!slot?.data?.length || !slot?.columns?.length) return [];
    const data = slot.data;
    const columns = slot.columns;
    const idx = this.selectedCleanedIndex();
    const search = (this.cleanedSearchTerms()[idx] ?? '').trim().toLowerCase();
    const sortState = this.cleanedSortState()[idx] ?? { columnKey: null, direction: 'asc' as const };
    let rows = search
      ? data.filter(row =>
        columns.some(col => String(row[col] ?? '').toLowerCase().includes(search))
      )
      : [...data];
    if (sortState.columnKey && columns.includes(sortState.columnKey)) {
      const col = sortState.columnKey;
      const numeric = isColumnNumeric(data, col);
      const dir = sortState.direction === 'asc' ? 1 : -1;
      rows = [...rows].sort((a, b) => {
        const va = a[col];
        const vb = b[col];
        if (numeric) {
          const na = Number(va);
          const nb = Number(vb);
          if (Number.isNaN(na) && Number.isNaN(nb)) return 0;
          if (Number.isNaN(na)) return 1;
          if (Number.isNaN(nb)) return -1;
          return dir * (na - nb);
        }
        const sa = String(va ?? '');
        const sb = String(vb ?? '');
        return dir * sa.localeCompare(sb, undefined, { sensitivity: 'base' });
      });
    }
    return rows;
  }

  setCleanedSearch(value: string): void {
    const idx = this.selectedCleanedIndex();
    this.cleanedSearchTerms.update(terms => {
      const next = [...terms];
      while (next.length <= idx) next.push('');
      next[idx] = value;
      return next;
    });
  }

  getCleanedSearch(): string {
    const idx = this.selectedCleanedIndex();
    return this.cleanedSearchTerms()[idx] ?? '';
  }

  setCleanedSort(columnKey: string): void {
    const idx = this.selectedCleanedIndex();
    this.cleanedSortState.update(state => {
      const next = [...state];
      while (next.length <= idx) next.push({ columnKey: null, direction: 'asc' });
      const prev = next[idx];
      const sameCol = prev.columnKey === columnKey;
      next[idx] = {
        columnKey,
        direction: sameCol && prev.direction === 'asc' ? 'desc' : 'asc'
      };
      return next;
    });
  }

  getCleanedSortForColumn(columnKey: string): 'asc' | 'desc' | null {
    const idx = this.selectedCleanedIndex();
    const s = this.cleanedSortState()[idx];
    if (!s || s.columnKey !== columnKey) return null;
    return s.direction;
  }

  isColumnNumeric(data: any[], columnKey: string): boolean {
    return isColumnNumeric(data, columnKey);
  }

  getCleanedPageIndex(): number {
    const idx = this.selectedCleanedIndex();
    return this.cleanedPageIndex()[idx] ?? 0;
  }

  getCleanedPageSize(): number {
    const idx = this.selectedCleanedIndex();
    return this.cleanedPageSize()[idx] ?? 25;
  }

  setCleanedPageIndex(page: number): void {
    const idx = this.selectedCleanedIndex();
    this.cleanedPageIndex.update(arr => {
      const next = [...arr];
      while (next.length <= idx) next.push(0);
      next[idx] = Math.max(0, page);
      return next;
    });
  }

  setCleanedPageSize(size: number): void {
    const idx = this.selectedCleanedIndex();
    this.cleanedPageSize.update(arr => {
      const next = [...arr];
      while (next.length <= idx) next.push(25);
      next[idx] = size;
      return next;
    });
    this.cleanedPageIndex.update(arr => {
      const next = [...arr];
      while (next.length <= idx) next.push(0);
      next[idx] = 0;
      return next;
    });
  }

  getCleanedTotalRows(slot: { data?: any[]; columns?: string[] } | null): number {
    return this.getCleanedTableData(slot).length;
  }

  getCleanedTotalPages(slot: { data?: any[]; columns?: string[] } | null): number {
    const total = this.getCleanedTotalRows(slot);
    const size = this.getCleanedPageSize();
    return size <= 0 ? 0 : Math.ceil(total / size);
  }

  getClampedCleanedPageIndex(slot: { data?: any[]; columns?: string[] } | null): number {
    const totalPages = this.getCleanedTotalPages(slot);
    return Math.min(Math.max(0, this.getCleanedPageIndex()), Math.max(0, totalPages - 1));
  }

  getCleanedPaginatedData(slot: { data?: any[]; columns?: string[] } | null): any[] {
    const full = this.getCleanedTableData(slot);
    const pageSize = this.getCleanedPageSize();
    const pageIndex = this.getClampedCleanedPageIndex(slot);
    const start = pageIndex * pageSize;
    return full.slice(start, start + pageSize);
  }

  /** Returns page indices to display; -1 means ellipsis. */
  getCleanedPageNumbers(totalPages: number): number[] {
    const current = this.getClampedCleanedPageIndex(this.currentCleanedSlot());
    if (totalPages <= 7) {
      return Array.from({ length: totalPages }, (_, i) => i);
    }
    const pages: number[] = [];
    if (current <= 3) {
      for (let i = 0; i < 5; i++) pages.push(i);
      pages.push(-1);
      pages.push(totalPages - 1);
    } else if (current >= totalPages - 4) {
      pages.push(0);
      pages.push(-1);
      for (let i = totalPages - 5; i < totalPages; i++) pages.push(i);
    } else {
      pages.push(0);
      pages.push(-1);
      for (let i = current - 1; i <= current + 1; i++) pages.push(i);
      pages.push(-1);
      pages.push(totalPages - 1);
    }
    return pages;
  }

  min(a: number, b: number): number {
    return Math.min(a, b);
  }
}
