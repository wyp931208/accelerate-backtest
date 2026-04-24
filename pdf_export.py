# -*- coding: utf-8 -*-
"""
PDF导出模块
将K线图表导出为PDF文件
"""
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Image, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os


def get_chinese_font():
    """尝试注册中文字体"""
    # 项目内字体目录（优先）
    project_font = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fonts', 'SimHei.ttf')
    font_paths = [
        project_font,
        'C:/Windows/Fonts/simhei.ttf',
        'C:/Windows/Fonts/msyh.ttc',
        'C:/Windows/Fonts/simsun.ttc',
        '/System/Library/Fonts/STHeiti Medium.ttc',
        '/Library/Fonts/Arial Unicode.ttf',
        '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
        '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',
        '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
        '/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf',
        '/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc',
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont('ChineseFont', fp))
                return 'ChineseFont'
            except Exception:
                continue
    # 兜底：尝试查找系统中任何可用的中文字体
    import glob
    for pattern in ['/usr/share/fonts/**/*CJK*', '/usr/share/fonts/**/*wqy*', '/usr/share/fonts/**/*Noto*SC*']:
        for fp in glob.glob(pattern, recursive=True):
            if fp.endswith(('.ttf', '.ttc', '.otf')):
                try:
                    pdfmetrics.registerFont(TTFont('ChineseFont', fp))
                    return 'ChineseFont'
                except Exception:
                    continue
    return 'Helvetica'


def export_charts_to_pdf(chart_buffers: list, stock_info: dict,
                          output_buffer: BytesIO = None) -> BytesIO:
    """
    将图表导出为PDF
    chart_buffers: list of BytesIO (PNG images)
    stock_info: dict with ts_code, name, etc.
    output_buffer: optional BytesIO to write to
    """
    if output_buffer is None:
        output_buffer = BytesIO()

    font_name = get_chinese_font()
    styles = getSampleStyleSheet()

    # 创建自定义样式
    title_style = ParagraphStyle(
        'ChineseTitle',
        parent=styles['Title'],
        fontName=font_name,
        fontSize=16,
        spaceAfter=6 * mm,
    )
    subtitle_style = ParagraphStyle(
        'ChineseSubtitle',
        parent=styles['Normal'],
        fontName=font_name,
        fontSize=10,
        spaceAfter=4 * mm,
    )

    doc = SimpleDocTemplate(
        output_buffer,
        pagesize=A4,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
    )

    story = []

    # 标题
    ts_code = stock_info.get('ts_code', '')
    name = stock_info.get('name', '')
    signal_date = stock_info.get('signal_date', '')

    story.append(Paragraph(f'{name} ({ts_code}) 加速策略分析报告', title_style))
    story.append(Paragraph(f'信号日期: {signal_date}', subtitle_style))

    # 股票基本信息表格
    info_data = [
        ['股票代码', ts_code, '股票名称', name],
        ['信号日期', str(signal_date), '板块', stock_info.get('board', '')],
    ]
    info_table = Table(info_data, colWidths=[30 * mm, 50 * mm, 30 * mm, 50 * mm])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), font_name),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 0), (0, -1), colors.Color(0.9, 0.95, 1)),
        ('BACKGROUND', (2, 0), (2, -1), colors.Color(0.9, 0.95, 1)),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 8 * mm))

    # 插入图表
    page_width = A4[0] - 30 * mm  # 减去左右边距
    for i, buf in enumerate(chart_buffers):
        chart_type = "日K线" if i == 0 else "周K线"
        story.append(Paragraph(f'{chart_type}技术分析图表', subtitle_style))
        buf.seek(0)
        try:
            img = Image(buf, width=page_width, height=page_width * 0.85)
            story.append(img)
        except Exception:
            story.append(Paragraph(f'[图表{i+1}生成失败]', subtitle_style))
        story.append(Spacer(1, 5 * mm))

    doc.build(story)
    output_buffer.seek(0)
    return output_buffer


def export_multi_stocks_pdf(stocks_data: list, output_buffer: BytesIO = None) -> BytesIO:
    """
    批量导出多只股票的PDF
    stocks_data: list of dict, each with keys:
        - chart_buffers: list of BytesIO
        - stock_info: dict
    """
    if output_buffer is None:
        output_buffer = BytesIO()

    font_name = get_chinese_font()
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'ChineseTitle',
        parent=styles['Title'],
        fontName=font_name,
        fontSize=16,
    )
    subtitle_style = ParagraphStyle(
        'ChineseSubtitle',
        parent=styles['Normal'],
        fontName=font_name,
        fontSize=10,
    )

    doc = SimpleDocTemplate(
        output_buffer,
        pagesize=A4,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
    )

    story = []
    story.append(Paragraph('XPK加速策略 - 信号股票分析报告', title_style))
    story.append(Spacer(1, 10 * mm))

    page_width = A4[0] - 30 * mm

    for stock_data in stocks_data:
        chart_buffers = stock_data.get('chart_buffers', [])
        stock_info = stock_data.get('stock_info', {})

        ts_code = stock_info.get('ts_code', '')
        name = stock_info.get('name', '')
        signal_date = stock_info.get('signal_date', '')

        story.append(Paragraph(f'{name} ({ts_code})', subtitle_style))

        for j, buf in enumerate(chart_buffers):
            chart_type = "日K线" if j == 0 else "周K线"
            buf.seek(0)
            try:
                img = Image(buf, width=page_width, height=page_width * 0.7)
                story.append(img)
            except Exception:
                pass

    doc.build(story)
    output_buffer.seek(0)
    return output_buffer
