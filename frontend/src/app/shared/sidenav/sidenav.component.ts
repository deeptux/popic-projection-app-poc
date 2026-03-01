import { Component, inject, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { BreakpointObserver } from '@angular/cdk/layout';
import { SidenavService } from './sidenav.service';

@Component({
  selector: 'app-sidenav',
  standalone: true,
  imports: [CommonModule, RouterModule],
  templateUrl: './sidenav.component.html',
  styleUrl: './sidenav.component.css',
})
export class SidenavComponent implements OnInit, OnDestroy {
  private readonly breakpointObserver = inject(BreakpointObserver);
  readonly sidenav = inject(SidenavService);

  /** True when viewport is lg (1024px) or wider — sidebar mode. */
  isDesktop = false;
  private sub: { unsubscribe: () => void } | null = null;

  ngOnInit() {
    const lgQuery = '(min-width: 1024px)';
    this.sub = this.breakpointObserver.observe(lgQuery).subscribe(state => {
      this.isDesktop = state.matches;
      if (!this.isDesktop) this.sidenav.closeDrawer();
    });
  }

  ngOnDestroy() {
    this.sub?.unsubscribe();
  }

  onToggle() {
    if (this.isDesktop) this.sidenav.toggleCollapsed();
    else this.sidenav.closeDrawer();
  }
}
