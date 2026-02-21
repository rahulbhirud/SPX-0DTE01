#!/usr/bin/env python3
"""
One-shot cleanup script: removes all divergence, plateau, and reversal code
from spx_stream.py while preserving the core RSI calculation.
"""
import re

FILE = "spx_stream.py"

with open(FILE, "r", encoding="utf-8") as f:
    lines = f.readlines()

# We'll work with 1-indexed line numbers for clarity, stored as list index 0-based
total = len(lines)
print(f"Read {total} lines from {FILE}")

# ─── Identify exact line ranges to remove/modify ──────────────────────

# Helper: find line number (1-based) containing a string
def find_line(text, start=0):
    for i in range(start, total):
        if text in lines[i]:
            return i + 1  # 1-based
    return None

# Helper: find line range for a method (from def/decorator to next def or class or section header)
def find_method_range(method_name, start_search=0):
    """Find the full extent of a method including its decorator comments."""
    start = None
    for i in range(start_search, total):
        if f"def {method_name}(" in lines[i]:
            start = i
            break
    if start is None:
        return None, None
    
    # Look backwards for section comment headers (lines starting with "    # ──")
    while start > 0 and (lines[start-1].strip().startswith("# ──") or 
                          lines[start-1].strip().startswith("#") and "──" in lines[start-1]):
        start -= 1
    # Also grab blank lines right before the comment block
    while start > 0 and lines[start-1].strip() == "":
        start -= 1
    start += 1  # don't eat the last content line
    
    # Find end: next def, class, or section header at same or lower indent
    indent = len(lines[start]) - len(lines[start].lstrip())
    # For methods, they'll be at 4-space indent
    method_indent = None
    for i in range(start, total):
        if f"def {method_name}(" in lines[i]:
            method_indent = len(lines[i]) - len(lines[i].lstrip())
            break
    
    end = None
    past_def = False
    for i in range(start, total):
        if f"def {method_name}(" in lines[i]:
            past_def = True
            continue
        if past_def:
            stripped = lines[i].strip()
            if stripped == "":
                continue
            line_indent = len(lines[i]) - len(lines[i].lstrip())
            # A new method/class/section at same or lower indent
            if (line_indent <= method_indent and 
                (stripped.startswith("def ") or stripped.startswith("class ") or 
                 stripped.startswith("# ══") or stripped.startswith("# ──"))):
                end = i  # exclusive (0-based)
                break
    if end is None:
        end = total
    
    return start, end  # 0-based, start inclusive, end exclusive


# Build a set of line indices (0-based) to REMOVE
remove = set()

# ── 1. Module docstring: remove plateau/divergence lines (5-6, 1-based) ──
# Line 5: "Calculates RSI(9) in real-time and monitors for:"
# Line 6: "  • RSI Plateau..."
# Line 7: "  • RSI Divergence..."
for i in range(total):
    if "RSI Plateau" in lines[i] or "RSI Divergence" in lines[i]:
        remove.add(i)
    if "monitors for:" in lines[i]:
        # Replace this line
        lines[i] = "Calculates RSI(9) in real-time.\n"

# ── 2. Config properties: remove plateau, signal_zone, divergence, reversal ──
config_props_to_remove = [
    "rsi_plateau_window", "rsi_plateau_threshold",
    "rsi_signal_zone_upper", "rsi_signal_zone_lower",
    "rsi_divergence_lookback_min", "rsi_divergence_lookback_max",
    "rsi_divergence_min_price_move_pct", "rsi_divergence_min_rsi_delta",
    "rsi_reversal_enabled", "rsi_reversal_window",
    "rsi_reversal_drop", "rsi_reversal_rise",
]

for prop_name in config_props_to_remove:
    for i in range(total):
        if f"def {prop_name}(" in lines[i]:
            # Find the @property decorator above (or docstring)
            prop_start = i
            # Look back for @property
            j = i - 1
            while j >= 0:
                s = lines[j].strip()
                if s == "@property":
                    prop_start = j
                    break
                if s == "" or s.startswith('"""') or s.startswith('#'):
                    j -= 1
                    continue
                break
            # Also grab blank line before @property
            if prop_start > 0 and lines[prop_start - 1].strip() == "":
                prop_start -= 1
            
            # Find end of property (return statement + optional blank line)
            prop_end = i + 1
            for k in range(i + 1, min(i + 10, total)):
                s = lines[k].strip()
                if s.startswith("return ") or s.startswith('"""'):
                    prop_end = k + 1
                elif s == "" and prop_end == k:
                    # blank line after return is fine, but don't extend further
                    break
                elif s.startswith("@property") or s.startswith("def ") or s.startswith("# ──"):
                    break
            
            for idx in range(prop_start, prop_end):
                remove.add(idx)
            break

# ── 3. RSISignal dataclass: remove entirely ──
for i in range(total):
    if "class RSISignal:" in lines[i] or "@dataclass" in lines[i]:
        pass  # we need to be careful - @dataclass is used for BarSnapshot too

# Find RSISignal specifically
rsi_signal_start = None
for i in range(total):
    if "class RSISignal:" in lines[i]:
        rsi_signal_start = i
        break

if rsi_signal_start is not None:
    # Look back for @dataclass
    j = rsi_signal_start - 1
    while j >= 0 and lines[j].strip() in ("", "@dataclass"):
        if lines[j].strip() == "@dataclass":
            rsi_signal_start = j
        j -= 1
    # Also grab preceding blank line  
    if rsi_signal_start > 0 and lines[rsi_signal_start - 1].strip() == "":
        rsi_signal_start -= 1
    
    # Find end of dataclass 
    rsi_signal_end = rsi_signal_start
    past_class = False
    for i in range(rsi_signal_start, total):
        if "class RSISignal:" in lines[i]:
            past_class = True
            continue
        if past_class:
            s = lines[i].strip()
            if s == "" or s.startswith("#") or s.startswith('"'):
                rsi_signal_end = i + 1
                continue
            indent = len(lines[i]) - len(lines[i].lstrip())
            if indent > 0 and not lines[i].startswith("class ") and not lines[i].startswith("# ══"):
                rsi_signal_end = i + 1
            else:
                break
    
    for idx in range(rsi_signal_start, rsi_signal_end):
        remove.add(idx)
    print(f"RSISignal: removing lines {rsi_signal_start+1}-{rsi_signal_end}")

# ── 4. RSIAnalyzer class docstring: simplify ──
# Find the docstring block and replace it
for i in range(total):
    if "class RSIAnalyzer:" in lines[i]:
        # Find the docstring
        for j in range(i+1, min(i+30, total)):
            if '"""' in lines[j] and "Tracks RSI" in lines[j]:
                # This is the opening triple-quote line
                doc_start = j
                # Find closing triple-quote
                for k in range(j+1, min(j+30, total)):
                    if '"""' in lines[k]:
                        doc_end = k
                        break
                # Replace entire docstring
                lines[doc_start] = '    """Tracks RSI(period) bar-by-bar using Wilder\'s smoothing."""\n'
                for idx in range(doc_start + 1, doc_end + 1):
                    remove.add(idx)
                break
        break

# ── 5. RSIAnalyzer.__init__: remove plateau/divergence/reversal vars ──
for i in range(total):
    if "def __init__(self, cfg: Config, logger: logging.Logger):" in lines[i] and i > 400:
        # Found RSIAnalyzer.__init__
        # Remove specific lines within __init__
        for j in range(i, min(i + 80, total)):
            line = lines[j]
            # Remove plateau/divergence/reversal config assignments
            if any(x in line for x in [
                "_plateau_window", "_plateau_thr", "_div_lb_min", "_div_lb_max",
                "_min_price_move", "_min_rsi_delta", "_zone_upper", "_zone_lower",
                "_rev_enabled", "_rev_window", "_rev_drop", "_rev_rise",
                "_last_plateau_bar", "_last_div_bar",
            ]):
                remove.add(j)
            
            # Simplify max_hist line
            if "max_hist = max(" in line:
                lines[j] = "        max_hist = self._period + 5\n"
            
            # Simplify the comment about history
            if "Closed-bar history for plateau / divergence / reversal checks" in line:
                lines[j] = "        # Closed-bar history for RSI display\n"
            
            # Stop at the next method
            if j > i + 5 and ("def " in line and "self" in line):
                break
        break

# ── 6. feed() method: remove signal generation calls ──
for i in range(total):
    if "def feed(self, candle: dict) -> List[RSISignal]:" in lines[i]:
        # Change return type
        lines[i] = lines[i].replace("-> List[RSISignal]:", "-> None:")
        
        # Remove signal-related lines in feed()
        for j in range(i, min(i + 60, total)):
            line = lines[j]
            if "signals: List[RSISignal] = []" in line:
                remove.add(j)
            if "signals += self._close_pending_bar()" in line:
                lines[j] = line.replace("signals += self._close_pending_bar()", "self._close_pending_bar()")
            if "signals += self._check_plateau" in line:
                remove.add(j)
            if "signals += self._check_divergence" in line:
                remove.add(j)
            if "signals += self._check_reversal" in line:
                remove.add(j)
            if "return signals" in line and "List" not in line:
                lines[j] = line.replace("return signals", "return")
            # Stop at next method definition
            if j > i + 5 and "    def " in line and line.strip().startswith("def "):
                break
        break

# ── 7. _close_pending_bar(): remove signal generation ──
for i in range(total):
    if "def _close_pending_bar(self) -> List[RSISignal]:" in lines[i]:
        lines[i] = lines[i].replace("-> List[RSISignal]:", "-> None:")
        
        for j in range(i, min(i + 30, total)):
            line = lines[j]
            if "signals: List[RSISignal] = []" in line:
                remove.add(j)
            if "signals += self._check_plateau" in line:
                remove.add(j)
            if "signals += self._check_divergence" in line:
                remove.add(j)
            if "signals += self._check_reversal" in line:
                remove.add(j)
            if line.strip() == "return signals":
                lines[j] = line.replace("return signals", "return")
            if j > i + 5 and "    def " in line and line.strip().startswith("def "):
                break
        break

# Also fix the docstring of _close_pending_bar
for i in range(total):
    if "Finalise the buffered open bar: advance RSI and run signal checks." in lines[i]:
        lines[i] = lines[i].replace(
            "Finalise the buffered open bar: advance RSI and run signal checks.",
            "Finalise the buffered open bar: advance RSI."
        )
        break

# ── 8. Remove _check_plateau() method entirely ──
s, e = find_method_range("_check_plateau", 600)
if s is not None:
    print(f"_check_plateau: removing lines {s+1}-{e}")
    for idx in range(s, e):
        remove.add(idx)

# ── 9. Remove _check_divergence() method entirely ──
s, e = find_method_range("_check_divergence", 600)
if s is not None:
    print(f"_check_divergence: removing lines {s+1}-{e}")
    for idx in range(s, e):
        remove.add(idx)

# ── 10. Remove _check_reversal() method entirely ──
s, e = find_method_range("_check_reversal", 600)
if s is not None:
    print(f"_check_reversal: removing lines {s+1}-{e}")
    for idx in range(s, e):
        remove.add(idx)

# ── 11. Remove _zone_label() method ──
s, e = find_method_range("_zone_label", 600)
if s is not None:
    print(f"_zone_label: removing lines {s+1}-{e}")
    for idx in range(s, e):
        remove.add(idx)

# ── 12. Simplify rsi_status_str() — remove _zone_label call ──
for i in range(total):
    if "def rsi_status_str(self)" in lines[i]:
        for j in range(i, min(i + 15, total)):
            if "_zone_label" in lines[j]:
                # Replace: return f"RSI({self._period})={rsi:6.2f}  {self._zone_label(rsi)}"
                lines[j] = '        zone = ""\n'
                # Check if the next line has the return with zone
                if j + 1 < total and "return" not in lines[j]:
                    pass  # zone_label was called inline
                # Actually, let me look at the actual code
        break

# More precise: find and replace the _zone_label usage in rsi_status_str
for i in range(total):
    if '_zone_label(rsi)' in lines[i] and 'rsi_status_str' not in lines[i]:
        # Replace the f-string to remove _zone_label call
        lines[i] = lines[i].replace('{self._zone_label(rsi)}', '')
        # Clean up trailing whitespace in the f-string
        lines[i] = lines[i].replace('  "', '"')
        break

# ── 13. SPXStreamer docstring: remove plateau/divergence mention ──
for i in range(total):
    if "plateau, and divergence logging" in lines[i]:
        lines[i] = lines[i].replace(
            "real-time RSI, plateau, and divergence logging",
            "real-time RSI logging"
        )
        break
    if "plateau" in lines[i].lower() and "divergence" in lines[i].lower() and "class SPXStreamer" not in lines[i]:
        if i > 1200:  # Only in SPXStreamer section
            lines[i] = lines[i].replace("RSI, plateau, and divergence", "RSI")
            break

# ── 14. SPXStreamer.__init__: remove _reversal_signals ──
for i in range(total):
    if "_reversal_signals" in lines[i] and "List[dict]" in lines[i]:
        remove.add(i)
        # Also remove the comment if on previous line
        if i > 0 and lines[i-1].strip().startswith("#"):
            pass  # leave comments for now
        break

# ── 15. SPXStreamer.on_candle(): remove signal emission ──
for i in range(total):
    if "def on_candle(self, candle: dict):" in lines[i]:
        for j in range(i, min(i + 30, total)):
            # Change: signals = self._rsi.feed(candle) → self._rsi.feed(candle)
            if "signals = self._rsi.feed(candle)" in lines[j]:
                lines[j] = lines[j].replace("signals = self._rsi.feed(candle)", "self._rsi.feed(candle)")
            # Remove signal iteration
            if "for sig in signals:" in lines[j]:
                remove.add(j)
            if "self._emit_signal(sig)" in lines[j]:
                remove.add(j)
            if j > i + 5 and "    def " in lines[j] and lines[j].strip().startswith("def "):
                break
        break

# ── 16. Remove _emit_signal() method entirely ──
s, e = find_method_range("_emit_signal", 1200)
if s is not None:
    print(f"_emit_signal: removing lines {s+1}-{e}")
    for idx in range(s, e):
        remove.add(idx)

# ── 17. _write_dashboard_state: remove signals ──
for i in range(total):
    if '"signals"' in lines[i] and "_reversal_signals" in lines[i]:
        remove.add(i)
        break

# Also remove _reversal_signals trimming code
for i in range(total):
    if "len(self._reversal_signals)" in lines[i]:
        # Remove the if block (2 lines)
        remove.add(i)
        if i + 1 < total and "_reversal_signals" in lines[i+1]:
            remove.add(i + 1)

# ── 18. main(): remove plateau/divergence/reversal logging ──
for i in range(total):
    if "Zones" in lines[i] and "signal_upper" in lines[i]:
        remove.add(i)
        if i + 1 < total:
            remove.add(i + 1)  # continuation line
    if "Plateau" in lines[i] and "window=" in lines[i] and "threshold" in lines[i]:
        remove.add(i)
        if i + 1 < total and ("cfg.rsi_plateau" in lines[i+1] or lines[i+1].strip().startswith("cfg.")):
            remove.add(i + 1)
    if "Divergence:" in lines[i] and "lookback=" in lines[i]:
        remove.add(i)
        # Check next 2 lines for continuation
        for k in range(i+1, min(i+4, total)):
            if "cfg.rsi_divergence" in lines[k] or lines[k].strip().startswith("cfg.rsi_divergence"):
                remove.add(k)
    if "Reversal:" in lines[i] and "window=" in lines[i] and "drop=" in lines[i]:
        remove.add(i)
        if i + 1 < total and ("cfg.rsi_reversal" in lines[i+1] or lines[i+1].strip().startswith("cfg.rsi_reversal")):
            remove.add(i + 1)

# More precise: find the exact main() logging lines
for i in range(total):
    if 'log.info("  Zones' in lines[i]:
        remove.add(i)
        if i + 1 < total and "cfg.rsi_signal_zone" in lines[i+1]:
            remove.add(i + 1)
    if 'log.info("  Plateau' in lines[i]:
        remove.add(i)
        if i + 1 < total and "cfg.rsi_plateau" in lines[i+1]:
            remove.add(i + 1)
    if 'log.info("  Divergence' in lines[i]:
        remove.add(i)
        for k in range(i+1, min(i+4, total)):
            if "cfg.rsi_divergence" in lines[k]:
                remove.add(k)
    if 'log.info("  Reversal' in lines[i]:
        remove.add(i)
        if i + 1 < total and "cfg.rsi_reversal" in lines[i+1]:
            remove.add(i + 1)

# ── 19. Clean up RSISignal import references ──
# The feed() method signature might reference RSISignal in type hints elsewhere
# Also remove "from typing import" if RSISignal was used - but RSISignal was local

# ── 20. Fix feed() docstring to not mention signals ──
for i in range(total):
    if "Feed a raw candle packet from the stream." in lines[i]:
        for j in range(i, min(i + 5, total)):
            if "Returns any RSISignal events" in lines[j]:
                remove.add(j)
            if j > i and lines[j].strip() == "":
                break
        break

# ── Build output ──────────────────────────────────────────────

output = []
for i in range(total):
    if i not in remove:
        output.append(lines[i])

# Clean up multiple consecutive blank lines (max 2)
cleaned = []
blank_count = 0
for line in output:
    if line.strip() == "":
        blank_count += 1
        if blank_count <= 2:
            cleaned.append(line)
    else:
        blank_count = 0
        cleaned.append(line)

# Write result
with open(FILE, "w", encoding="utf-8") as f:
    f.writelines(cleaned)

print(f"\nDone! Wrote {len(cleaned)} lines (removed {total - len(cleaned)} lines)")
print(f"Lines marked for removal: {len(remove)}")
