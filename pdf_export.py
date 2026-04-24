# -*- coding: utf-8 -*-
"""
PDF导出模块
将K线图表导出为PDF文件
每页只放一个图表（日K线或周K线），日周分开，排版美观
"""
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import (SimpleDocTemplate, Image, Paragraph, Spacer,
                                 Table, TableStyle, PageBreak)
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


def _build_pdf(stocks_data: list, output_buffer: BytesIO = None) -> BytesIO:
    """
    通用PDF构建函数
    每只股票的日K线和周K线各占一页，排版清晰美观
    """
    if output_buffer is None:
        output_buffer = BytesIO()

    font_name = get_chinese_font()
    styles = getSampleStyleSheet()

    # 自定义样式
    title_style = ParagraphStyle(
        'ChineseTitle', parent=styles['Title'],
        fontName=font_name, fontSize=18, spaceAfter=4 * mm,
        alignment=1,  # 居中
    )
    subtitle_style = ParagraphStyle(
        'ChineseSubtitle', parent=styles['Normal'],
        fontName=font_name, fontSize=10, spaceAfter=3 * mm,
        alignment=1,
    )
    chart_title_daily = ParagraphStyle(
        'ChartTitleDaily', parent=styles['Normal'],
        fontName=font_name, fontSize=12, spaceAfter=2 * mm,
        textColor=colors.HexColor('#ef5350'),  # 日线-红色标题
    )
    chart_title_weekly = ParagraphStyle(
        'ChartTitleWeekly', parent=styles['Normal'],
        fontName=font_name, fontSize=12, spaceAfter=2 * mm,
        textColor=colors.HexColor('#1565C0'),  # 周线-蓝色标题
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
    page_width = A4[0] - 30 * mm  # 可用宽度
    chart_height = page_width * 0.82  # 图表高度，让一页正好放下

    is_multi = len(stocks_data) > 1

    # 多股票时添加总封面
    if is_multi:
        story.append(Spacer(1, 60 * mm))
        story.append(Paragraph('XPK加速策略', title_style))
        story.append(Paragraph('信号股票分析报告', ParagraphStyle(
            'SubTitle2', parent=styles['Normal'],
            fontName=font_name, fontSize=14, spaceAfter=8 * mm, alignment=1,
        )))
        story.append(Spacer(1, 10 * mm))
        # 股票列表
        for sd in stocks_data:
            info = sd.get('stock_info', {})
            line = f"{info.get('name', '')} ({info.get('ts_code', '')})  板块: {info.get('board', '')}  信号日: {info.get('signal_date', '')}"
            story.append(Paragraph(line, ParagraphStyle(
                'StockList', parent=styles['Normal'],
                fontName=font_name, fontSize=10, spaceAfter=3 * mm, alignment=1,
            )))
        story.append(PageBreak())

    for idx, stock_data in enumerate(stocks_data):
        chart_buffers = stock_data.get('chart_buffers', [])
        stock_info = stock_data.get('stock_info', {})

        ts_code = stock_info.get('ts_code', '')
        name = stock_info.get('name', '')
        signal_date = stock_info.get('signal_date', '')
        board = stock_info.get('board', '')

        # 每只股票的开头（第一页：信息 + 日K线）
        # 股票名称标题
        story.append(Paragraph(f'{name} ({ts_code})', title_style))
        story.append(Paragraph(f'信号日期: {signal_date}  |  板块: {board}', subtitle_style))
        story.append(Spacer(1, 2 * mm))

        # 基本信息表格
        info_data = [
            ['股票代码', ts_code, '股票名称', name],
            ['信号日期', str(signal_date), '板块', board],
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
        story.append(Spacer(1, 4 * mm))

        # ---- 日K线图（第一页） ----
        if len(chart_buffers) >= 1:
            story.append(Paragraph('▶ 日K线技术分析（红绿配色）', chart_title_daily))
            story.append(Spacer(1, 2 * mm))
            buf = chart_buffers[0]
            buf.seek(0)
            try:
                img = Image(buf, width=page_width, height=chart_height)
                story.append(img)
            except Exception:
                story.append(Paragraph('[日K线图表生成失败]', subtitle_style))

        # ---- 周K线图（第二页） ----
        if len(chart_buffers) >= 2:
            story.append(PageBreak())
            story.append(Paragraph(f'{name} ({ts_code})', subtitle_style))
            story.append(Spacer(1, 2 * mm))
            story.append(Paragraph('▶ 周K线技术分析（蓝橙配色）', chart_title_weekly))
            story.append(Spacer(1, 2 * mm))
            buf = chart_buffers[1]
            buf.seek(0)
            try:
                img = Image(buf, width=page_width, height=chart_height)
                story.append(img)
            except Exception:
                story.append(Paragraph('[周K线图表生成失败]', subtitle_style))

        # 多股票时，每只股票之间分页
        if idx < len(stocks_data) - 1:
            story.append(PageBreak())

    doc.build(story)
    output_buffer.seek(0)
    return output_buffer


def export_charts_to_pdf(chart_buffers: list, stock_info: dict,
                          output_buffer: BytesIO = None) -> BytesIO:
    """
    单只股票导出PDF
    """
    return _build_pdf([{
        'chart_buffers': chart_buffers,
        'stock_info': stock_info,
    }], output_buffer)


def export_multi_stocks_pdf(stocks_data: list, output_buffer: BytesIO = None) -> BytesIO:
    """
    批量导出多只股票的PDF
    """
    return _build_pdf(stocks_data, output_buffer)
