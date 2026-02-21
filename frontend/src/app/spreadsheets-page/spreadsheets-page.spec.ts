import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SpreadsheetsPage } from './spreadsheets-page';

describe('spreadsheetsPage', () => {
  let component: SpreadsheetsPage;
  let fixture: ComponentFixture<SpreadsheetsPage>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SpreadsheetsPage]
    }).compileComponents();

    fixture = TestBed.createComponent(SpreadsheetsPage);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
