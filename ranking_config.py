"""
排行榜配置系統
支援靈活的指標組合和權重設定
"""

# 基礎指標定義
BASE_INDICATORS = {
    'all_ROI': '全期間年化報酬率',
    '30d_ROI': '30天年化報酬率', 
    '14d_ROI': '14天年化報酬率',
    '7d_ROI': '7天年化報酬率',
    '2d_ROI': '2天年化報酬率',
    '1d_ROI': '1天年化報酬率',
    'all_return': '全期間累積報酬',
    '30d_return': '30天累積報酬',
    '14d_return': '14天累積報酬',
    '7d_return': '7天累積報酬',
    '2d_return': '2天累積報酬',
    '1d_return': '1天累積報酬'
}

# 排行榜策略配置
RANKING_STRATEGIES = {
    'original': {
        'name': '原始策略',
        'components': {
            'long_term_score': {
                'indicators': ['1d_ROI', '2d_ROI', '7d_ROI', '14d_ROI', '30d_ROI', 'all_ROI'],
                'weights': [1, 1, 1, 1, 1, 1],  # 等權重
                'normalize': True  # 是否標準化
            },
            'short_term_score': {
                'indicators': ['1d_ROI', '2d_ROI', '7d_ROI', '14d_ROI'],
                'weights': [1, 1, 1, 1],
                'normalize': True
            }
        },
        'final_combination': {
            'scores': ['long_term_score', 'short_term_score'],
            'weights': [0.5, 0.5]  # 最終組合權重
        }
    },
    
    'momentum_focused': {
        'name': '動量導向策略',
        'components': {
            'short_momentum': {
                'indicators': ['1d_ROI', '2d_ROI'],
                'weights': [0.6, 0.4],  # 更重視近期
                'normalize': True
            },
            'medium_momentum': {
                'indicators': ['7d_ROI', '14d_ROI'],
                'weights': [0.6, 0.4],
                'normalize': True
            }
        },
        'final_combination': {
            'scores': ['short_momentum', 'medium_momentum'],
            'weights': [0.7, 0.3]  # 更重視短期動量
        }
    },
    
    'stability_focused': {
        'name': '穩定性導向策略',
        'components': {
            'consistency_score': {
                'indicators': ['14d_ROI', '30d_ROI', 'all_ROI'],
                'weights': [0.4, 0.4, 0.2],  # 重視中長期穩定性
                'normalize': True
            },
            'recent_performance': {
                'indicators': ['1d_ROI', '2d_ROI', '7d_ROI'],
                'weights': [0.2, 0.3, 0.5],  # 遞增權重
                'normalize': True
            }
        },
        'final_combination': {
            'scores': ['consistency_score', 'recent_performance'],
            'weights': [0.6, 0.4]  # 更重視穩定性
        }
    },
    
    'adaptive': {
        'name': '自適應策略',
        'components': {
            'volatility_adjusted': {
                'indicators': ['1d_ROI', '7d_ROI', '30d_ROI'],
                'weights': [0.3, 0.4, 0.3],
                'normalize': True,
                'volatility_penalty': True  # 根據波動率調整
            }
        },
        'final_combination': {
            'scores': ['volatility_adjusted'],
            'weights': [1.0]
        }
    },
    
    'pure_short_term': {
        'name': '純短期策略',
        'components': {
            'daily_focus': {
                'indicators': ['1d_ROI', '2d_ROI'],
                'weights': [0.8, 0.2],  # 極度重視1天表現
                'normalize': True
            }
        },
        'final_combination': {
            'scores': ['daily_focus'],
            'weights': [1.0]
        }
    },
    
    'balanced': {
        'name': '平衡策略',
        'components': {
            'short_term': {
                'indicators': ['1d_ROI', '2d_ROI', '7d_ROI'],
                'weights': [0.5, 0.3, 0.2],
                'normalize': True
            },
            'medium_term': {
                'indicators': ['14d_ROI', '30d_ROI'],
                'weights': [0.6, 0.4],
                'normalize': True
            },
            'long_term': {
                'indicators': ['all_ROI'],
                'weights': [1.0],
                'normalize': True
            }
        },
        'final_combination': {
            'scores': ['short_term', 'medium_term', 'long_term'],
            'weights': [0.5, 0.3, 0.2]  # 平衡各個時間段
        }
    }
}

# 默認策略
DEFAULT_STRATEGY = 'original'

# 實驗性策略配置 - 方便快速測試
EXPERIMENTAL_CONFIGS = {
    'test_1': {
        'name': '測試1: 純短期',
        'components': {
            'ultra_short': {
                'indicators': ['1d_ROI'],
                'weights': [1.0],
                'normalize': False
            }
        },
        'final_combination': {
            'scores': ['ultra_short'],
            'weights': [1.0]
        }
    },
    
    'test_2': {
        'name': '測試2: 長短期平衡',
        'components': {
            'short': {
                'indicators': ['1d_ROI', '2d_ROI'],
                'weights': [0.6, 0.4],
                'normalize': True
            },
            'long': {
                'indicators': ['30d_ROI', 'all_ROI'],
                'weights': [0.7, 0.3],
                'normalize': True
            }
        },
        'final_combination': {
            'scores': ['short', 'long'],
            'weights': [0.8, 0.2]  # 偏重短期
        }
    },
    
    'test_3': {
        'name': '測試3: 反向權重',
        'components': {
            'reverse_momentum': {
                'indicators': ['1d_ROI', '7d_ROI', '30d_ROI'],
                'weights': [0.1, 0.3, 0.6],  # 越長期權重越高
                'normalize': True
            }
        },
        'final_combination': {
            'scores': ['reverse_momentum'],
            'weights': [1.0]
        }
    },
    
    'test_4': {
        'name': '測試4: 極端動量',
        'components': {
            'extreme_short': {
                'indicators': ['1d_ROI'],
                'weights': [1.0],
                'normalize': True
            },
            'extreme_long': {
                'indicators': ['all_ROI'],
                'weights': [1.0],
                'normalize': True
            }
        },
        'final_combination': {
            'scores': ['extreme_short', 'extreme_long'],
            'weights': [0.9, 0.1]  # 極度偏重短期
        }
    },
    
    # ================================================================
    # 極簡測試策略 - 用於手工驗證計算過程
    # ================================================================
    
    'test_simple_1d': {
        'name': '極簡測試1: 純1天ROI，無標準化',
        'components': {
            'simple_1d': {
                'indicators': ['1d_ROI'],
                'weights': [1.0],
                'normalize': False  # 關鍵：不標準化，直接使用原值
            }
        },
        'final_combination': {
            'scores': ['simple_1d'],
            'weights': [1.0]
        }
    },
    
    'test_simple_avg': {
        'name': '極簡測試2: 1天+2天ROI平均，無標準化',
        'components': {
            'simple_avg': {
                'indicators': ['1d_ROI', '2d_ROI'],
                'weights': [0.5, 0.5],  # 等權重平均
                'normalize': False  # 不標準化，便於手工計算
            }
        },
        'final_combination': {
            'scores': ['simple_avg'],
            'weights': [1.0]
        }
    },
    
    'test_normalize_1d': {
        'name': '極簡測試3: 純1天ROI，有標準化',
        'components': {
            'normalized_1d': {
                'indicators': ['1d_ROI'],
                'weights': [1.0],
                'normalize': True  # 測試標準化邏輯
            }
        },
        'final_combination': {
            'scores': ['normalized_1d'],
            'weights': [1.0]
        }
    },
    
    'test_weighted_simple': {
        'name': '極簡測試4: 1天ROI*0.7 + 2天ROI*0.3，無標準化',
        'components': {
            'weighted_simple': {
                'indicators': ['1d_ROI', '2d_ROI'],
                'weights': [0.7, 0.3],  # 不等權重
                'normalize': False
            }
        },
        'final_combination': {
            'scores': ['weighted_simple'],
            'weights': [1.0]
        }
    },
    
    'test_two_components': {
        'name': '極簡測試5: 兩組件組合，無標準化',
        'components': {
            'comp_1d': {
                'indicators': ['1d_ROI'],
                'weights': [1.0],
                'normalize': False
            },
            'comp_2d': {
                'indicators': ['2d_ROI'],
                'weights': [1.0],
                'normalize': False
            }
        },
        'final_combination': {
            'scores': ['comp_1d', 'comp_2d'],
            'weights': [0.6, 0.4]  # 最終組合權重
        }
    }
}

# 策略說明函數
def get_strategy_description(strategy_name):
    """獲取策略的詳細說明"""
    strategies = {**RANKING_STRATEGIES, **EXPERIMENTAL_CONFIGS}
    
    if strategy_name not in strategies:
        return f"未知策略: {strategy_name}"
    
    strategy = strategies[strategy_name]
    desc = [f"策略名稱: {strategy['name']}"]
    desc.append("=" * 40)
    
    # 組件說明
    for comp_name, comp_config in strategy['components'].items():
        desc.append(f"組件: {comp_name}")
        desc.append(f"  指標: {', '.join(comp_config['indicators'])}")
        desc.append(f"  權重: {comp_config['weights']}")
        desc.append(f"  標準化: {'是' if comp_config.get('normalize', False) else '否'}")
        if comp_config.get('volatility_penalty', False):
            desc.append("  特殊: 波動率懲罰")
        desc.append("")
    
    # 最終組合說明
    final = strategy['final_combination']
    desc.append("最終組合:")
    for score, weight in zip(final['scores'], final['weights']):
        desc.append(f"  {score}: {weight*100:.1f}%")
    
    return "\n".join(desc)

# 列出所有可用策略
def list_all_strategies():
    """列出所有可用的策略"""
    print("🎯 主要策略:")
    for name, config in RANKING_STRATEGIES.items():
        print(f"  📋 {name}: {config['name']}")
    
    print("\n🧪 實驗策略:")
    for name, config in EXPERIMENTAL_CONFIGS.items():
        print(f"  🔬 {name}: {config['name']}")

if __name__ == "__main__":
    # 測試用
    list_all_strategies()
    print("\n" + "="*50)
    print(get_strategy_description('original')) 