"""
Run from repo root: python patch_bet_button_f5_ml.py

Adds the missing <BetButton/> wiring to F5Row and MLRow. EdgeRow was already
done. Patterns match the actual current code which includes ReasonToggle
inside cell-conviction and the <></> fragment wrapper with ReasonDetail.
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
app = app_path.read_text(encoding="utf-8")

# ============================================================================
# 1. F5Row — close pattern with ReasonToggle inside cell-conviction
# ============================================================================
old_f5_close = """      <div className="cell-conviction">
        {conv!=null
          ? <span className="conv-value">{Number(conv).toFixed(0)}% <span className="tier-pill">T{tier}</span></span>
          : <span className="conv-na">n/a</span>}
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
  );
}"""

new_f5_close = """      <div className="cell-conviction">
        {conv!=null
          ? <span className="conv-value">{Number(conv).toFixed(0)}% <span className="tier-pill">T{tier}</span></span>
          : <span className="conv-na">n/a</span>}
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
      <div className="cell-bet"><BetButton edge={edge}/></div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
  );
}"""

if old_f5_close in app:
    app = app.replace(old_f5_close, new_f5_close, 1)
    print("OK: F5Row gets <BetButton/>")
else:
    # Already done? Check
    f5_section = app[app.index("function F5Row"):app.index("function MLRow")]
    if 'className="cell-bet"' in f5_section:
        print("OK: F5Row already has BetButton")
    else:
        print("WARN: F5Row close pattern not found — pasted code didn't match")

# ============================================================================
# 2. MLRow — tier-pill + ReasonToggle inside a div, then close
# ============================================================================
# Visible structure from your Select-String:
#   <div>
#     <span className="tier-pill">T{tier}</span>
#     <ReasonToggle .../>
#   </div>
# </div>  <-- closes ml-row
# {open && hasFactors && <ReasonDetail .../>}
# </>

old_ml_close = """      <div>
        <span className="tier-pill">T{tier}</span>
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
  );
}"""

new_ml_close = """      <div>
        <span className="tier-pill">T{tier}</span>
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
      <div className="cell-bet"><BetButton edge={edge}/></div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
  );
}"""

if old_ml_close in app:
    app = app.replace(old_ml_close, new_ml_close, 1)
    print("OK: MLRow gets <BetButton/>")
else:
    ml_idx = app.index("function MLRow")
    nxt = app.find("\nfunction ", ml_idx + 1)
    ml_section = app[ml_idx:nxt if nxt != -1 else len(app)]
    if 'className="cell-bet"' in ml_section:
        print("OK: MLRow already has BetButton")
    else:
        print("WARN: MLRow close pattern not found")

app_path.write_text(app, encoding="utf-8")

print()
print("Verify with:")
print('  Select-String -Path frontend\\src\\App.jsx -Pattern "BetButton edge=" -Context 0,0')
print("  (should now show 3 matches)")
print()
print("Then build and push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx patch_bet_button_f5_ml.py")
print("  git commit -m 'BetButton: wire into F5Row and MLRow'")
print("  git push")
