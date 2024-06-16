from django.shortcuts import render, redirect
from .forms import PairsForm
from .forms import KeysForm, Bot_Path_Form
from .models import Pairs, Keys, BotPath
from .bot.bot_launch import Bot_launch
from .bot.Sql_query import SQLQuery
from django.views.generic import UpdateView, DeleteView
from django.http import JsonResponse
from collections import deque

bot_launch = Bot_launch()
sqlQuery = SQLQuery()


def bot(request):

    if request.is_ajax():
        try:
            message_status, message_progress = sqlQuery.get_progress_status_engine()
        except:
            status_text = ""
            progress_status = 0
        return JsonResponse({'message_progress': message_progress, 'message_status': message_status})

    try:
        on_start = sqlQuery.get_status_engine()
    except:
        on_start = 0
    if request.method == 'POST':
        if on_start:
            bot_launch.stop_bot()
            return redirect('bot')
        else:
            bot_launch.start_bot()
            return redirect('bot')

    profit_table = sqlQuery.get_profit_table()
    data = {'title': 'Bot', 'on_start': on_start, 'profit_table': profit_table}
    return render(request, 'main/bot.html', data)


def orders(request):
    orders_ = sqlQuery.get_all_orders_modif()
    data = {'orders': orders_}
    return render(request, 'main/orders.html', data)


def pairs(request):
    error = ''

    if request.method == 'POST':
        form = PairsForm(request.POST)
        if form.is_valid():
            form.save()
        else:
            error = "Error: wrong form"

    form = PairsForm()
    pairs_ = Pairs.objects.all()
    data = {'form': form, 'error': error, 'pairs': pairs_}
    return render(request, 'main/pairs.html', data)


class PairUpdateView(UpdateView):
    model = Pairs
    template_name = 'main/pair_update_view.html'

    form_class = PairsForm


class PairDeleteView(DeleteView):
    model = Pairs
    success_url = '/pairs'
    template_name = 'main/pair_delete_view.html'

    fields = ['Pair_Name']


def toggle_active(request, pk):
    pair = Pairs.objects.get(pk=pk)
    pair.active = not pair.active
    pair.save()
    return redirect('/pairs')

def logs(request):

    logs_info_file = "logs/info.log"
    logs_warning_file = "logs/warning.log"
    logs_error_file = "logs/error.log"
    with open(logs_info_file) as f:
        logs_info = list(deque(f, 70))
    with open(logs_warning_file) as f:
        logs_warning = list(deque(f, 30))
    with open(logs_error_file) as f:
        logs_error = list(deque(f, 30))

    data_logs = {'logs_info': logs_info, 'logs_warning': logs_warning, 'logs_error': logs_error}
    return render(request, 'main/logs.html', data_logs)


def settings(request):
    error_key, error_bots = '', ''

    if request.method == 'POST':
        form_type = request.POST.get('form_type')
        if form_type == 'keys_form':
            keys_form = KeysForm(request.POST)
            if keys_form.is_valid():
                keys_form.save()
            else:
                error_key = "Error"
        elif form_type == 'bot_path_form':
            bot_path_form = Bot_Path_Form(request.POST)
            if bot_path_form.is_valid():
                bot_path_form.save()
            else:
                error_bots = "Error"

    # создаем формы с начальными значениями из базы данных
    keys_form = KeysForm()
    bot_path = BotPath.objects.last()  # получаем последнюю запись из таблицы Bot_Path
    bot_path_initial = {'Bot_Path_Body': bot_path.Bot_Path_Body,
                        'Bot_Path_Gen': bot_path.Bot_Path_Gen} if bot_path else {}
    bot_path_form = Bot_Path_Form(initial=bot_path_initial)

    data = {'keys_form': keys_form, 'bot_path_form': bot_path_form, 'error_key': error_key, 'error_bots': error_bots}

    return render(request, 'main/settings.html', data)

