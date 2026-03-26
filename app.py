from flask import Flask, render_template_string, send_file, flash, redirect, url_for
import os
from nota import rodar_extracao

app = Flask(__name__)
app.secret_key = os.urandom(24) # Chave segura para mensagens na tela

# Interface HTML Simples usando Bootstrap
HTML_PAGE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Extrator de Notas - Abastecimento</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background-color: #f4f6f9; padding-top: 50px; }
        .card { border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
    </style>
</head>
<body>
    <div class="container d-flex justify-content-center">
        <div class="col-md-6 text-center">
            <div class="card p-5">
                <h2 class="mb-4">🚛 Extrator de NF-e (Diesel e ARLA)</h2>
                <p class="text-muted mb-4">Clique no botão abaixo para consultar a SEFAZ. O sistema irá baixar todas as notas novas emitidas desde a última consulta.</p>
                
                {% with messages = get_flashed_messages(with_categories=true) %}
                  {% if messages %}
                    {% for category, message in messages %}
                      <div class="alert alert-{{ category }}">{{ message }}</div>
                    {% endfor %}
                  {% endif %}
                {% endwith %}

                <form action="{{ url_for('executar_extracao') }}" method="POST">
                    <button type="submit" class="btn btn-primary btn-lg w-100" id="btn-rodar">
                        Consultar SEFAZ e Baixar CSV
                    </button>
                </form>
            </div>
        </div>
    </div>

    <script>
        // Muda o texto do botão para mostrar que está carregando
        document.querySelector('form').addEventListener('submit', function() {
            var btn = document.getElementById('btn-rodar');
            btn.innerHTML = 'Consultando SEFAZ... Aguarde ⏳';
            btn.classList.add('disabled');
        });
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_PAGE)

@app.route('/executar', methods=['POST'])
def executar_extracao():
    try:
        # Chama a função principal do seu script original
        arquivo_csv = rodar_extracao()
        
        if arquivo_csv and os.path.exists(arquivo_csv):
            # Se deu certo, envia o arquivo para download automático
            return send_file(arquivo_csv, as_attachment=True)
        else:
            # Se não tem notas novas
            flash("A varredura terminou, mas nenhuma nota nova de abastecimento foi encontrada.", "warning")
            return redirect(url_for('index'))
            
    except Exception as e:
        flash(f"Ocorreu um erro: {str(e)}", "danger")
        return redirect(url_for('index'))

if __name__ == '__main__':
    # Roda o servidor na porta 5000
    app.run(debug=True, port=5000)