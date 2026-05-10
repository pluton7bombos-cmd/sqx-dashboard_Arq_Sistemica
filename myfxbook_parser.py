#!/usr/bin/env python3
"""
Myfxbook CSV Parser
Convierte CSV de Myfxbook en JSON estructurado por bot (Magic Number)
Extrae métricas directamente del CSV (sin recalcular)
Integrable con db_data.json del dashboard SQX

Autor: Fede (Plutón 7 Bombos)
Uso: python myfxbook_parser.py --input statement.csv --output output.json
"""

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import argparse

# Magic Number Mapper: Original → Normalizado (VPS migration)
MAGIC_MAPPER = {
    4914115: 4914,    # AUDUSD
    2623214: 2623,    # AUDUSD
    1555115: 1555,    # EURUSD
    # Agregar más según sea necesario
}

def normalize_magic(magic):
    """Normaliza Magic Number (maneja VPS migrations)"""
    if magic in MAGIC_MAPPER:
        return MAGIC_MAPPER[magic]
    return magic

def parse_csv(csv_path):
    """Lee CSV de Myfxbook y retorna lista de trades"""
    trades = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Filtrar depósitos y filas vacías
                if row.get('Action') == 'Deposit' or not row.get('Ticket'):
                    continue
                
                # Normalizar Magic Number
                magic = int(row.get('Magic Number', 0))
                magic_normalized = normalize_magic(magic)
                
                # Parsear el trade
                try:
                    trade = {
                        'ticket': int(row.get('Ticket', 0)),
                        'open_date': row.get('Open Date', ''),
                        'close_date': row.get('Close Date', ''),
                        'symbol': row.get('Symbol', ''),
                        'action': row.get('Action', ''),
                        'lots': float(row.get('Units/Lots', 0)),
                        'open_price': float(row.get('Open Price', 0)),
                        'close_price': float(row.get('Close Price', 0)),
                        'pips': float(row.get('Pips', 0)),
                        'profit': float(row.get('Profit', 0)),
                        'gain_percent': float(row.get('Gain', 0)),
                        'profitable': row.get('Profitable(%)', '0') != '0',
                        'drawdown': float(row.get('Drawdown', 0)),
                        'magic': magic_normalized,
                        'comment': row.get('Comment', ''),
                    }
                    trades.append(trade)
                except (ValueError, KeyError) as e:
                    print(f"⚠️  Skipping malformed trade: {row.get('Ticket')} - {e}")
                    continue
    except FileNotFoundError:
        print(f"❌ Error: Archivo no encontrado: {csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error al leer CSV: {e}")
        sys.exit(1)
    
    return trades

def group_by_magic(trades):
    """Agrupa trades por Magic Number"""
    bots = defaultdict(list)
    for trade in trades:
        magic = trade['magic']
        bots[magic].append(trade)
    return dict(bots)

def calculate_bot_metrics(trades, bot_magic):
    """
    Calcula métricas agregadas por bot usando datos del CSV
    Extrae directamente sin recalcular
    """
    if not trades:
        return None
    
    # Conteos básicos
    total_trades = len(trades)
    winning_trades = [t for t in trades if t['profit'] > 0]
    losing_trades = [t for t in trades if t['profit'] < 0]
    break_even = [t for t in trades if t['profit'] == 0]
    
    winning_count = len(winning_trades)
    losing_count = len(losing_trades)
    
    # Sumas
    total_profit = sum(t['profit'] for t in trades)
    total_wins = sum(t['profit'] for t in winning_trades)
    total_losses = sum(t['profit'] for t in losing_trades)
    
    # Promedios
    avg_win = total_wins / winning_count if winning_count > 0 else 0
    avg_loss = abs(total_losses) / losing_count if losing_count > 0 else 0
    
    # Profit Factor
    pf = total_wins / abs(total_losses) if total_losses != 0 else (float('inf') if total_wins > 0 else 0)
    
    # Winning Rate %
    wr = (winning_count / total_trades * 100) if total_trades > 0 else 0
    
    # Drawdown máximo (del CSV, campo por trade)
    max_dd = max([t['drawdown'] for t in trades]) if trades else 0
    
    # Symbols únicos
    symbols = list(set(t['symbol'] for t in trades))
    primary_symbol = symbols[0] if symbols else 'UNKNOWN'
    
    # Fechas
    first_trade = min(trades, key=lambda t: t['open_date'])
    last_trade = max(trades, key=lambda t: t['close_date'])
    
    return {
        'magic': bot_magic,
        'symbol': primary_symbol,
        'strategy': trades[0].get('comment', 'N/A'),
        'metrics': {
            'trades': total_trades,
            'winning': winning_count,
            'losing': losing_count,
            'break_even': len(break_even),
            'profit_factor': round(pf, 2) if pf != float('inf') else 999.99,
            'winning_rate': round(wr, 2),
            'total_profit': round(total_profit, 2),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'drawdown_percent': round(max_dd, 2),
        },
        'first_trade_date': first_trade['open_date'],
        'last_trade_date': last_trade['close_date'],
        'trades': trades  # Incluir trades completos para equity curve
    }

def generate_equity_curve(trades):
    """
    Genera equity curve agregada por fecha (Close Date)
    Asume equity inicial = 10000 (por defecto)
    """
    INITIAL_EQUITY = 10000
    equity_by_date = {}
    running_equity = INITIAL_EQUITY
    
    # Ordenar trades por close date
    sorted_trades = sorted(trades, key=lambda t: t['close_date'])
    
    for trade in sorted_trades:
        date = trade['close_date'].split()[0]  # Extraer solo la fecha (YYYY-MM-DD)
        running_equity += trade['profit']
        equity_by_date[date] = running_equity
    
    # Convertir a lista
    equity_curve = [
        {
            'date': date,
            'equity': equity,
            'profit_accumulated': equity - INITIAL_EQUITY
        }
        for date, equity in sorted(equity_by_date.items())
    ]
    
    return equity_curve

def build_myfxbook_json(bots_data):
    """Construye JSON final de Myfxbook (sin SQX data)"""
    myfxbook_json = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'total_bots': len(bots_data),
            'source': 'myfxbook_csv_parser'
        },
        'bots': []
    }
    
    for bot_magic, trades in bots_data.items():
        metrics = calculate_bot_metrics(trades, bot_magic)
        
        if not metrics:
            continue
        
        equity_curve = generate_equity_curve(trades)
        
        bot_entry = {
            'id': str(bot_magic),
            'magic': bot_magic,
            'symbol': metrics['symbol'],
            'strategy': metrics['strategy'],
            'myfxbook': {
                'trades': metrics['metrics']['trades'],
                'winning': metrics['metrics']['winning'],
                'losing': metrics['metrics']['losing'],
                'break_even': metrics['metrics']['break_even'],
                'profit_factor': metrics['metrics']['profit_factor'],
                'winning_rate': metrics['metrics']['winning_rate'],
                'total_profit': metrics['metrics']['total_profit'],
                'avg_win': metrics['metrics']['avg_win'],
                'avg_loss': metrics['metrics']['avg_loss'],
                'drawdown_percent': metrics['metrics']['drawdown_percent'],
                'equity_curve': equity_curve,
                'first_trade': metrics['first_trade_date'],
                'last_trade': metrics['last_trade_date'],
            }
        }
        
        myfxbook_json['bots'].append(bot_entry)
    
    return myfxbook_json

def merge_with_sqx_db(myfxbook_json, db_data_path=None):
    """
    Intenta mergear con db_data.json existente (preserva SQX data)
    Si db_data.json no existe, retorna JSON puro de Myfxbook
    """
    if not db_data_path or not Path(db_data_path).exists():
        return myfxbook_json
    
    try:
        with open(db_data_path, 'r', encoding='utf-8-sig') as f:
            db_data = json.load(f)
    except Exception as e:
        print(f"⚠️  No se pudo leer db_data.json: {e}. Retornando JSON puro.")
        return myfxbook_json

    # db_data puede ser lista directa o dict con clave 'bots'
    if isinstance(db_data, list):
        sqx_list = db_data
    else:
        sqx_list = db_data.get('bots', [])

    # Crear índice por magic number (solo bots que tienen magic definido)
    sqx_bots = {}
    for bot in sqx_list:
        m = bot.get('magic')
        if m is not None:
            sqx_bots[str(m)] = bot

    merged_bots = list(sqx_list)  # copia de todos los bots SQX

    for myfx_bot in myfxbook_json['bots']:
        magic = str(myfx_bot['magic'])

        if magic in sqx_bots:
            # Bot existe en SQX: inyectar myfxbook data directamente
            sqx_bots[magic]['myfxbook'] = myfx_bot['myfxbook']
            print(f"   ✔ Magic {magic} mergeado con SQX bot existente")
        else:
            # Bot nuevo (no está en SQX): agregar como entrada nueva
            merged_bots.append(myfx_bot)
            print(f"   + Magic {magic} agregado como bot nuevo (sin SQX data)")

    # Retornar misma estructura que db_data original
    if isinstance(db_data, list):
        return merged_bots
    else:
        db_data['bots'] = merged_bots
        return db_data

def main():
    parser = argparse.ArgumentParser(
        description='Parsea CSV de Myfxbook y genera JSON para dashboard'
    )
    parser.add_argument('--input', '-i', required=True, help='Ruta del CSV de Myfxbook')
    parser.add_argument('--output', '-o', required=True, help='Ruta del JSON output')
    parser.add_argument('--merge-db', '-m', help='Ruta de db_data.json existente (opcional, para mergear SQX data)')
    parser.add_argument('--validate-only', action='store_true', help='Solo validar, no guardar')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🚀 Myfxbook CSV Parser")
    print("=" * 60)
    
    # Paso 1: Parsear CSV
    print(f"\n📖 Leyendo CSV: {args.input}")
    trades = parse_csv(args.input)
    print(f"✅ {len(trades)} trades encontrados")
    
    # Paso 2: Agrupar por Magic Number
    print(f"\n🔀 Agrupando por Magic Number...")
    bots_data = group_by_magic(trades)
    print(f"✅ {len(bots_data)} bots encontrados:")
    for magic, bot_trades in bots_data.items():
        print(f"   • Magic {magic}: {len(bot_trades)} trades")
    
    # Paso 3: Generar JSON
    print(f"\n📊 Generando JSON...")
    myfxbook_json = build_myfxbook_json(bots_data)
    
    # Paso 4: Mergear con SQX (si existe)
    if args.merge_db:
        print(f"\n🔗 Mergeando con SQX data: {args.merge_db}")
        final_json = merge_with_sqx_db(myfxbook_json, args.merge_db)
    else:
        final_json = myfxbook_json
    
    # Paso 5: Validación
    bot_list = final_json if isinstance(final_json, list) else final_json.get('bots', [])
    print(f"\n✔️  Validación:")
    print(f"   • Total bots: {len(bot_list)}")
    for bot in bot_list[:3]:  # Mostrar primeros 3
        myfx = bot.get('myfxbook', {})
        if myfx:
            print(f"   • Magic {bot['magic']}: {myfx.get('trades', 0)} trades, PF {myfx.get('profit_factor', 'N/A')}")
    
    if args.validate_only:
        print(f"\n✅ Validación OK (--validate-only, no se guardó nada)")
        return
    
    # Paso 6: Guardar output
    print(f"\n💾 Guardando: {args.output}")
    try:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_json, f, indent=2, ensure_ascii=False)
        print(f"✅ Guardado OK")
    except Exception as e:
        print(f"❌ Error al guardar: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("✅ DONE!")
    print("=" * 60)

if __name__ == '__main__':
    main()
