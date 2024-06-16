from .models import Pairs
from .models import Keys, BotPath
from django.forms import ModelForm, TextInput
from cryptography.fernet import Fernet


class PairsForm(ModelForm):
    class Meta:
        model = Pairs
        fields = ['Pair_Name', 'Pair_quote', 'Pair_base', 'Spend_amount', 'active']

        widgets = {
            "Pair_Name": TextInput(attrs={
                'class': "form-control",
                'placeholder': "Pair Name",
            }),
            "Pair_quote": TextInput(attrs={
                'class': "form-control",
                'placeholder': "Pair quote"
            }),
            "Pair_base": TextInput(attrs={
                'class': "form-control",
                'placeholder': "Pair base"
            }),
            "Spend_amount": TextInput(attrs={
                'class': "form-control",
                'placeholder': "Spend Amount"
            }),
        }


class KeysForm(ModelForm):
    class Meta:
        model = Keys
        fields = ['API_KEY', 'API_SECRET']

        widgets = {
            "API_KEY": TextInput(attrs={
                'class': "form-control",
                'placeholder': "API KEY",
                'autocomplete': 'off'  # добавляем атрибут autocomplete
            }),
            "API_SECRET": TextInput(attrs={
                'class': "form-control",
                'placeholder': "API SECRET",
                'autocomplete': 'off'  # добавляем атрибут autocomplete
            }),
        }

    def save(self, commit=True):
        # Получаем объект модели Keys из формы
        keys = super(KeysForm, self).save(commit=False)

        # Шифруем данные API_KEY и API_SECRET
        key = Fernet.generate_key()

        # сохранение ключа в файл
        with open('buffer/k.k', 'wb') as key_file:
            key_file.write(key)

        fernet = Fernet(key)
        api_key = fernet.encrypt(self.cleaned_data['API_KEY'].encode())
        api_secret = fernet.encrypt(self.cleaned_data['API_SECRET'].encode())

        # Сохраняем зашифрованные данные в модель Keys
        keys.API_KEY = api_key.decode()
        keys.API_SECRET = api_secret.decode()

        if commit:
            keys.save()
        return keys


class Bot_Path_Form(ModelForm):
    class Meta:
        model = BotPath
        fields = ['Bot_Path_Body', 'Bot_Path_Gen']

        widgets = {
            "Bot_Path_Body": TextInput(attrs={
                'class': "form-control",
                'placeholder': "Введите имя бота из bot storage"
            }),
            "Bot_Path_Gen": TextInput(attrs={
                'class': "form-control",
                'placeholder': "Введите имя гена из bot storage"
            }),
        }
