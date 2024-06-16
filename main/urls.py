from django.urls import path
from . import views

urlpatterns = [
    path('', views.bot, name='bot'),
    path('orders/', views.orders, name='orders'),
    path('pairs/', views.pairs, name='pairs'),
    path('pairs/<int:pk>/update', views.PairUpdateView.as_view(), name='pair-update'),
    path('pairs/<int:pk>/delete', views.PairDeleteView.as_view(), name='pair-delete'),
    path('toggle_active/<int:pk>/', views.toggle_active, name='toggle_active'),
    path('logs/', views.logs, name='logs'),
    path('settings/', views.settings, name='settings')
]
