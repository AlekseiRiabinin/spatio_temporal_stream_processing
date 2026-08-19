import graphviz
import yaml
from pathlib import Path


def extract_services_from_compose(compose_path):
    """Extract services from docker-compose file"""
    with open(compose_path, 'r') as f:
        compose_data = yaml.safe_load(f)
    
    services = set()
    if 'services' in compose_data:
        services.update(compose_data['services'].keys())
    
    return sorted(services)

def extract_relationships(compose_path):
    """Extract service relationships from docker-compose depends_on"""
    relationships = []
    with open(compose_path, 'r') as f:
        compose_data = yaml.safe_load(f)
    
    if 'services' in compose_data:
        for service, config in compose_data['services'].items():
            if 'depends_on' in config:
                if isinstance(config['depends_on'], list):
                    for dependency in config['depends_on']:
                        relationships.append((service, dependency))
                elif isinstance(config['depends_on'], dict):
                    for dependency in config['depends_on'].keys():
                        relationships.append((service, dependency))
    
    return relationships

def extract_ports(compose_path):
    """Extract port mappings from compose file"""
    ports = {}
    with open(compose_path, 'r') as f:
        compose_data = yaml.safe_load(f)
    
    if 'services' in compose_data:
        for service, config in compose_data['services'].items():
            if 'ports' in config:
                ports[service] = []
                for port in config['ports']:
                    ports[service].append(port)
    
    return ports

def build_diagram(services, relationships, ports):
    """Generate CityRover system architecture diagram"""
    dot = graphviz.Digraph('CityRoverArchitecture', 
        format='png',
        graph_attr={
            'rankdir': 'TB',  # Top to bottom layout
            'fontname': 'Helvetica',
            'splines': 'ortho',
            'nodesep': '0.5',
            'ranksep': '0.6',
            'newrank': 'true'
        },
        node_attr={
            'fontname': 'Helvetica',
            'style': 'filled',
            'fontsize': '10'
        })
    
    # ===== DATA STORAGE GROUP =====
    with dot.subgraph(name='cluster_storage') as c:
        c.attr(label='Data Storage', 
              style='filled,rounded', 
              color='#e6f3ff',
              fontsize='12')
        
        if 'postgis-real-deal' in services:
            c.node('postgis-real-deal', 
                   shape='cylinder', 
                   fillcolor='#cce6ff',
                   label='PostGIS\n(5436:5432)')
        
        if 'graph-data' in services:
            c.node('graph-data', 
                   shape='folder', 
                   fillcolor='#f0f0f0',
                   label='Graph Data\n(Bind Mount)')
        
        if 'trajectory-data' in services:
            c.node('trajectory-data', 
                   shape='folder', 
                   fillcolor='#f0f0f0',
                   label='Trajectory Data\n(Bind Mount)')
    
    # ===== MESSAGE QUEUE GROUP =====
    with dot.subgraph(name='cluster_messaging') as c:
        c.attr(label='Message Queue', 
              style='filled,rounded', 
              color='#ffe6e6',
              fontsize='12')
        
        if 'kafka-1' in services:
            c.node('kafka-1', 
                   shape='box3d', 
                   fillcolor='#ffb3b3',
                   label='Kafka Broker\n(19092:19092)')
    
    # ===== PROCESSING GROUP =====
    with dot.subgraph(name='cluster_processing') as c:
        c.attr(label='Batch Processing', 
              style='filled,rounded', 
              color='#e6f7ff',
              fontsize='12')
        
        if 'graph-engine' in services:
            c.node('graph-engine', 
                   shape='box3d', 
                   fillcolor='#ffcc99',
                   label='Graph Engine\n(JVM)\nBuilds Road Graph')
        
        if 'rover-simulator' in services:
            c.node('rover-simulator', 
                   shape='box3d', 
                   fillcolor='#ffdd99',
                   label='Rover Simulator\n(JVM)\nGenerates Telemetry')
        
        if 'trajectory-visualizer-job' in services:
            c.node('trajectory-visualizer-job', 
                   shape='box3d', 
                   fillcolor='#99ccff',
                   label='Trajectory Visualizer\n(Spark Job)\nProcesses Telemetry')
    
    # ===== VISUALIZATION GROUP =====
    with dot.subgraph(name='cluster_visualization') as c:
        c.attr(label='Visualization', 
              style='filled,rounded', 
              color='#f0fff0',
              fontsize='12')
        
        if 'rover-map-visualizer' in services:
            c.node('rover-map-visualizer', 
                   shape='component', 
                   fillcolor='#99ff99',
                   label='Map Visualizer\n(8080:8080)\nHTTP Server')
    
    # ===== DATA FLOW RELATIONSHIPS =====
    
    # PostGIS to Graph Engine (data source)
    if 'postgis-real-deal' in services and 'graph-engine' in services:
        dot.edge('postgis-real-deal', 'graph-engine', 
                label='reads OSM', 
                color='blue',
                fontsize='9')
    
    # Graph Engine to Graph Data (output)
    if 'graph-engine' in services and 'graph-data' in services:
        dot.edge('graph-engine', 'graph-data', 
                label='writes graph', 
                color='green',
                style='dashed',
                fontsize='9')
    
    # Graph Data to Rover Simulator (input)
    if 'graph-data' in services and 'rover-simulator' in services:
        dot.edge('graph-data', 'rover-simulator', 
                label='reads graph', 
                color='green',
                style='dashed',
                fontsize='9')
    
    # Rover Simulator to Kafka (producer)
    if 'rover-simulator' in services and 'kafka-1' in services:
        dot.edge('rover-simulator', 'kafka-1', 
                label='produces telemetry', 
                color='orange',
                fontsize='9')
    
    # Kafka to Trajectory Visualizer (consumer)
    if 'kafka-1' in services and 'trajectory-visualizer-job' in services:
        dot.edge('kafka-1', 'trajectory-visualizer-job', 
                label='consumes telemetry', 
                color='purple',
                fontsize='9')
    
    # Trajectory Visualizer to Trajectory Data (output)
    if 'trajectory-visualizer-job' in services and 'trajectory-data' in services:
        dot.edge('trajectory-visualizer-job', 'trajectory-data', 
                label='writes visualization', 
                color='green',
                style='dashed',
                fontsize='9')
    
    # Trajectory Data to Map Visualizer (serves)
    if 'trajectory-data' in services and 'rover-map-visualizer' in services:
        dot.edge('trajectory-data', 'rover-map-visualizer', 
                label='serves data', 
                color='blue',
                style='dashed',
                fontsize='9')
    
    return dot

if __name__ == "__main__":
    # Paths
    base_dir = Path(__file__).parent.resolve()
    compose_path = base_dir / "docker-compose.viz.yml"
    output_dir = base_dir / "output"
    
    # Verify files exist
    if not compose_path.exists():
        print(f"Error: Compose file not found at {compose_path}")
        print(f"Looking in: {base_dir}/docker-compose.viz.yml")
        exit(1)
    
    # Extract data
    try:
        services = extract_services_from_compose(compose_path)
        relationships = extract_relationships(compose_path)
        ports = extract_ports(compose_path)
    except Exception as e:
        print(f"Error processing compose file: {e}")
        exit(1)
    
    # Generate diagram
    diagram = build_diagram(services, relationships, ports)
    
    # Render
    try:
        output_path = diagram.render(output_dir / 'app_diagram', 
                                    view=False, 
                                    cleanup=True)
        print(f"Successfully generated system diagram: {output_path}")
    except Exception as e:
        print(f"Error generating diagram: {e}")
        exit(1)
