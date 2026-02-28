import {
  Component,
  input,
  signal,
  computed,
  inject,
  HostListener,
  ElementRef,
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatTableModule } from '@angular/material/table';
import { FormsModule } from '@angular/forms';

/** Format numeric values with commas and 2 decimal places. Skip number formatting for excluded columns (e.g. Year). */
function formatCellValue(val: unknown, excludeFromNumberFormat?: boolean): string {
  if (val === null || val === undefined) return '';
  if (excludeFromNumberFormat) return String(val);
  const n = Number(val);
  if (!Number.isNaN(n)) {
    return n.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 0 });
  }
  return String(val);
}

function isColumnNumeric(data: any[], columnKey: string): boolean {
  for (const row of data) {
    const v = row[columnKey];
    if (v === null || v === undefined || v === '') continue;
    const n = Number(v);
    if (Number.isNaN(n)) return false;
  }
  return true;
}

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

@Component({
  selector: 'app-spreadsheet-data-table',
  standalone: true,
  imports: [CommonModule, MatTableModule, FormsModule],
  templateUrl: './spreadsheet-data-table.component.html',
  styleUrl: './spreadsheet-data-table.component.css',
})
export class SpreadsheetDataTableComponent {
  private readonly hostRef = inject(ElementRef);

  readonly data = input.required<any[]>();
  readonly columns = input.required<string[]>();
  readonly title = input<string>('Results');
  /** Column keys that show a filter dropdown instead of sort (e.g. ['Salesperson']). */
  readonly filterableColumns = input<string[]>([]);
  /** Column keys that should not be formatted with commas (e.g. ['Year']). Case-insensitive. */
  readonly excludeFormatColumns = input<string[]>([]);
  /** Column keys whose numeric value is shown as percentage (value × 100 + '%'). Case-insensitive. */
  readonly percentageColumns = input<string[]>([]);

  readonly searchTerm = signal('');
  readonly sortColumn = signal<string | null>(null);
  readonly sortDirection = signal<'asc' | 'desc'>('asc');
  readonly pageIndex = signal(0);
  readonly pageSize = signal(25);
  /** Selected values per column key for filterable columns. Empty = show all. */
  readonly columnFilters = signal<Record<string, Set<string>>>({});
  readonly filterDropdownOpen = signal<string | null>(null);
  /** Search term inside the open filter dropdown (filters the list of values). */
  readonly filterDropdownSearchTerm = signal('');
  /** Position for the fixed filter dropdown (set when opening from button). */
  readonly filterDropdownPosition = signal<{ top: number; left: number } | null>(null);

  readonly filteredData = computed(() => {
    const raw = this.data();
    const cols = this.columns();
    const search = this.searchTerm().trim().toLowerCase();
    const filters = this.columnFilters();
    if (!raw?.length || !cols?.length) return [];
    let rows = raw;
    if (search) {
      rows = rows.filter(row =>
        cols.some(col => String(row[col] ?? '').toLowerCase().includes(search))
      );
    }
    for (const col of cols) {
      if (!this.isFilterable(col)) continue;
      const selected = filters[col];
      if (selected && selected.size > 0) {
        rows = rows.filter(row => selected.has(String(row[col] ?? '')));
      }
    }
    return rows;
  });

  readonly sortedData = computed(() => {
    const rows = this.filteredData();
    const cols = this.columns();
    const col = this.sortColumn();
    const dir = this.sortDirection();
    if (!col || !cols.includes(col) || rows.length === 0) return rows;
    const numeric = isColumnNumeric(this.data(), col);
    const mult = dir === 'asc' ? 1 : -1;
    return [...rows].sort((a, b) => {
      const va = a[col];
      const vb = b[col];
      if (numeric) {
        const na = Number(va);
        const nb = Number(vb);
        if (Number.isNaN(na) && Number.isNaN(nb)) return 0;
        if (Number.isNaN(na)) return 1;
        if (Number.isNaN(nb)) return -1;
        return mult * (na - nb);
      }
      const sa = String(va ?? '');
      const sb = String(vb ?? '');
      return mult * sa.localeCompare(sb, undefined, { sensitivity: 'base' });
    });
  });

  readonly totalRows = computed(() => this.filteredData().length);
  readonly totalPages = computed(() => {
    const total = this.totalRows();
    const size = this.pageSize();
    return size <= 0 ? 0 : Math.ceil(total / size);
  });
  readonly clampedPageIndex = computed(() => {
    const pages = this.totalPages();
    return Math.min(Math.max(0, this.pageIndex()), Math.max(0, pages - 1));
  });
  readonly paginatedData = computed(() => {
    const rows = this.sortedData();
    const size = this.pageSize();
    const idx = this.clampedPageIndex();
    const start = idx * size;
    return rows.slice(start, start + size);
  });

  readonly pageNumbers = computed(() => {
    const total = this.totalPages();
    const current = this.clampedPageIndex();
    if (total <= 7) return Array.from({ length: total }, (_, i) => i);
    const out: number[] = [];
    if (current <= 3) {
      for (let i = 0; i < 5; i++) out.push(i);
      out.push(-1);
      out.push(total - 1);
    } else if (current >= total - 4) {
      out.push(0);
      out.push(-1);
      for (let i = total - 5; i < total; i++) out.push(i);
    } else {
      out.push(0);
      out.push(-1);
      for (let i = current - 1; i <= current + 1; i++) out.push(i);
      out.push(-1);
      out.push(total - 1);
    }
    return out;
  });

  readonly rangeStart = computed(() => {
    const total = this.totalRows();
    if (total === 0) return 0;
    return this.clampedPageIndex() * this.pageSize() + 1;
  });
  readonly rangeEnd = computed(() =>
    Math.min(this.clampedPageIndex() * this.pageSize() + this.pageSize(), this.totalRows())
  );

  formatCellValue(val: unknown, columnKey: string): string {
    const isPercentage = this.percentageColumns().some(
      key => key.toLowerCase() === columnKey.toLowerCase()
    );
    if (isPercentage) {
      if (val === null || val === undefined || val === '') return '';
      const n = Number(val);
      if (!Number.isNaN(n)) {
        const pct = n * 100;
        return pct.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 0 }) + '%';
      }
    }
    const exclude = this.excludeFormatColumns().some(
      key => key.toLowerCase() === columnKey.toLowerCase()
    );
    return formatCellValue(val, exclude);
  }
  isColumnNumeric(col: string): boolean {
    return isColumnNumeric(this.data(), col);
  }
  readonly pageSizeOptions = PAGE_SIZE_OPTIONS;

  setSearch(value: string): void {
    this.searchTerm.set(value);
    this.pageIndex.set(0);
  }

  toggleSort(columnKey: string): void {
    if (this.isFilterable(columnKey)) return;
    const current = this.sortColumn();
    const dir = this.sortDirection();
    if (current === columnKey) {
      // Tri-state: asc -> desc -> null (original order)
      if (dir === 'asc') {
        this.sortDirection.set('desc');
      } else {
        this.sortColumn.set(null);
        this.sortDirection.set('asc');
      }
    } else {
      this.sortColumn.set(columnKey);
      this.sortDirection.set('asc');
    }
    this.pageIndex.set(0);
  }

  /** Returns current sort direction for column, or null if not sorted by this column. */
  getSortDirection(col: string): 'asc' | 'desc' | null {
    return this.sortColumn() === col ? this.sortDirection() : null;
  }

  isFilterable(col: string): boolean {
    return this.filterableColumns().some(key => key.toLowerCase() === col.toLowerCase());
  }

  getUniqueValues(col: string): string[] {
    const raw = this.data();
    const set = new Set<string>();
    for (const row of raw) {
      const v = row[col];
      if (v !== null && v !== undefined && v !== '') set.add(String(v).trim());
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  }

  /** Unique values for the given column, filtered by the dropdown search term (for display in dropdown). */
  getFilteredUniqueValues(col: string): string[] {
    const all = this.getUniqueValues(col);
    const search = this.filterDropdownSearchTerm().trim().toLowerCase();
    if (!search) return all;
    return all.filter(v => v.toLowerCase().includes(search));
  }

  setFilterDropdownSearch(value: string): void {
    this.filterDropdownSearchTerm.set(value);
  }

  isFilterValueSelected(col: string, value: string): boolean {
    return (this.columnFilters()[col] ?? new Set()).has(value);
  }

  toggleFilterValue(col: string, value: string): void {
    this.columnFilters.update(filters => {
      const next = { ...filters };
      const set = new Set(next[col] ?? []);
      if (set.has(value)) set.delete(value);
      else set.add(value);
      next[col] = set;
      return next;
    });
    this.pageIndex.set(0);
  }

  clearFilter(col: string): void {
    this.columnFilters.update(f => {
      const next = { ...f };
      delete next[col];
      return next;
    });
    this.filterDropdownOpen.set(null);
    this.filterDropdownPosition.set(null);
    this.filterDropdownSearchTerm.set('');
    this.pageIndex.set(0);
  }

  openFilterDropdown(col: string, event?: Event): void {
    if (event) {
      event.stopPropagation();
    }
    const current = this.filterDropdownOpen();
    if (current === col) {
      this.filterDropdownOpen.set(null);
      this.filterDropdownPosition.set(null);
      this.filterDropdownSearchTerm.set('');
      return;
    }
    this.filterDropdownSearchTerm.set('');
    let position: { top: number; left: number } | null = null;
    const el = event?.currentTarget ?? event?.target;
    if (el && el instanceof HTMLElement) {
      const rect = el.getBoundingClientRect();
      position = { top: rect.bottom + 4, left: rect.left };
    }
    this.filterDropdownPosition.set(position);
    this.filterDropdownOpen.set(col);
  }

  setPageIndex(idx: number): void {
    this.pageIndex.set(Math.max(0, Math.min(idx, this.totalPages() - 1)));
  }

  setPageSize(size: number): void {
    // #region agent log
    const before = this.pageSize();
    // #endregion
    const validOptions = this.pageSizeOptions;
    const sizeNum = Number(size);
    const valid = Number.isFinite(sizeNum) && sizeNum > 0 && validOptions.includes(sizeNum);
    const newSize = valid ? sizeNum : (validOptions.includes(before) ? before : validOptions[0]);
    this.pageSize.set(newSize);
    this.pageIndex.set(0);
    // #region agent log
    fetch('http://127.0.0.1:7300/ingest/4ad0fa15-8d7c-4c81-9308-f31565d9bdbb', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Debug-Session-Id': 'dfa0e3' }, body: JSON.stringify({ sessionId: 'dfa0e3', location: 'spreadsheet-data-table.component.ts:setPageSize', message: 'setPageSize', data: { title: this.title(), sizeParam: size, newSize, pageSizeBefore: before, pageSizeAfter: this.pageSize(), totalPagesAfter: this.totalPages(), paginatedLen: this.paginatedData().length }, timestamp: Date.now(), hypothesisId: 'H3,H4,H5' }) }).catch(() => { });
    // #endregion
  }

  min(a: number, b: number): number {
    return Math.min(a, b);
  }

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    if (!this.filterDropdownOpen()) return;
    const el = this.hostRef.nativeElement as HTMLElement;
    if (el.contains(event.target as Node)) return;
    this.filterDropdownOpen.set(null);
    this.filterDropdownPosition.set(null);
    this.filterDropdownSearchTerm.set('');
  }
}
