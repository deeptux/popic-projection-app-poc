import { Injectable, signal, computed } from '@angular/core';

/** Manages sidenav collapse (desktop) and drawer open (mobile/tablet). */
@Injectable({ providedIn: 'root' })
export class SidenavService {
  /** Desktop: sidebar is collapsed (icon-only). Mobile: not used. */
  readonly isCollapsed = signal(false);

  /** Mobile/tablet: drawer is open. */
  readonly isDrawerOpen = signal(false);

  readonly toggleCollapsed = () => this.isCollapsed.update(v => !v);
  readonly openDrawer = () => this.isDrawerOpen.set(true);
  readonly closeDrawer = () => this.isDrawerOpen.set(false);
  readonly toggleDrawer = () => this.isDrawerOpen.update(v => !v);
}
