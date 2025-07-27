import streamlit as st
import pandas as pd
import numpy as np
import pickle

# --- THIS MUST BE THE VERY FIRST STREAMLIT COMMAND IN YOUR SCRIPT ---
st.set_page_config(
    page_title="Job Title Predictor 🚀",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)
# -------------------------------------------------------------------


title_mapping = {
    0: "Java",
    1: "Data_Analyst",
    2: "Test_Engineer",
    3: "Software_Engineer",
}


location_options = ['Banglore', 'Chennai', 'Delhi', 'Gurugram', 'Hyderabad', 'Kolkata', 'Mumbai', 'Pune']
# Creating a dictionary to map string locations to their integer indices (0, 1, 2, ...)
location_to_int_map = {location: i for i, location in enumerate(location_options)}


# --- 1. Load your trained model ---
model = None # Initialize model to None
expected_columns_for_df = [] # Initialize this list to be populated after model load

try:
    with open('model_Title_Classification.pkl', 'rb') as file:
        model = pickle.load(file)
    st.success("Model loaded successfully!")

    # Use model's expected features for column order (no longer displaying on UI)
    if hasattr(model, 'feature_names_in_'):
        expected_columns_for_df = model.feature_names_in_.tolist()
    else:
        # Fallback to the manually defined list for feature order
        # This list MUST match X.columns.tolist() from your training script *after* encoding Location
        expected_columns_for_df = [
            'Location', 'Java', 'Min Experience', 'Max Experience', 'Min Salary', 'Max Salary',
            'Spring Boot', 'SQL', 'Git', 'Microservices', 'Hibernate', 'Power_BI',
            'Excel', 'Python', 'Visualization', 'spark', 'Cloud_Computing',
            'manual_testing', 'regression_testing', 'automation_testing',
            'software_testing', 'Kubernetes', 'Docker', 'Jenkins', 'CI/CD'
        ]

except FileNotFoundError:
    st.error("Error: Model file 'your_model.pkl' not found. Please ensure your trained model is in the same directory.")
    st.stop()
except Exception as e:
    st.error(f"Error loading model: {e}. Please check your model file.")
    st.stop()

# Ensure model is loaded and expected_columns_for_df is populated
if model is None or not expected_columns_for_df:
    st.error("Model failed to load or expected features could not be determined. Please check the error messages above.")
    st.stop()


# --- 2. Streamlit App Title and Description ---
# st.set_page_config is at the very top of the script.
st.title("👨‍💻 Predict Your Job Title! 👩‍💻")
st.markdown("""
    This application predicts the most likely job title based on various skills, experience, and salary expectations.
    Fill in the details below and let our ML model do the magic! ✨
""")

st.markdown("---")

# --- 3. Input Features Collection ---

st.header("Job Details and Skills")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Experience & Salary")
    min_experience = st.slider("Minimum Experience (Years)", 0, 30, 2)
    max_experience = st.slider("Maximum Experience (Years)", 0, 30, 7)
    min_salary = st.slider("Minimum Salary (LPA - Lakhs per Annum)", 0, 100, 5, step=1)
    max_salary = st.slider("Maximum Salary (LPA - Lakhs per Annum)", 0, 100, 15, step=1)

with col2:
    st.subheader("Location")
    # Using the hardcoded location_options list directly
    selected_location = st.selectbox("Select Preferred Job Location", location_options)

st.markdown("---")

st.subheader("Technical Skills")
skill_cols = st.columns(4)

skill_values = {}
all_skill_names = [
    'Java', 'Spring Boot', 'SQL', 'Git', 'Microservices', 'Hibernate',
    'Power_BI', 'Excel', 'Python', 'Visualization', 'spark',
    'Cloud_Computing', 'manual_testing', 'regression_testing',
    'automation_testing', 'software_testing', 'Kubernetes', 'Docker',
    'Jenkins', 'CI/CD'
]

skill_index = 0
for skill_name in all_skill_names:
    with skill_cols[skill_index % len(skill_cols)]:
        skill_values[skill_name] = st.checkbox(skill_name.replace('_', ' '))
    skill_index += 1

st.markdown("---")

# --- 4. Prediction Button and Logic ---

if st.button("Predict Job Title 🚀", help="Click to see the predicted job title"):
    input_data = {
        'Min Experience': min_experience,
        'Max Experience': max_experience,
        'Min Salary': min_salary,
        'Max Salary': max_salary,
    }

    # IMPORTANT CHANGE: Use the hardcoded map to get the numerical location
    encoded_location = location_to_int_map.get(selected_location)
    if encoded_location is None:
        st.error(f"Error: Location '{selected_location}' not found in the hardcoded mapping. Please select a valid location.")
        st.stop()
    input_data['Location'] = encoded_location


    input_data.update(skill_values)

    # Create DataFrame from input data
    model_input_df = pd.DataFrame([input_data])

    # Reorder and ensure all expected columns are present, filling missing with 0
    # This is a robust way to ensure column order and presence
    for col in expected_columns_for_df:
        if col not in model_input_df.columns:
            model_input_df[col] = 0 # Numerical defaults

    # Ensure the DataFrame has exactly the right columns in the right order
    model_input_df = model_input_df[expected_columns_for_df]


    st.markdown("### Prediction Result:")
    try:
        predicted_label = model.predict(model_input_df)[0]
        predicted_title = title_mapping.get(predicted_label, "Unknown Title")

        if predicted_title == "Unknown Title":
            st.warning(f"Model predicted an unknown label: {predicted_label}. Please check your `title_mapping` dictionary.")
        else:
            st.success(f"The predicted job title is: **{predicted_title}**")
            st.balloons()

    except Exception as e:
        st.error(f"An error occurred during prediction: {e}")
        st.warning("Please ensure your input data exactly matches the features your model was trained on.")


st.markdown("---")
st.markdown("""
    <p style='text-align: center; color: grey;'>
        Built with ❤️ using Streamlit and Machine Learning.
    </p>""", unsafe_allow_html=True)

# --- 5. Styling with Markdown (Optional but Recommended) ---
st.markdown(
    """
    <style>
    .stApp {
        background-color: #f0f2f6;
        color: #333333;
    }
    .css-1d391kg {
        color: #2e86de;
    }
    .stButton>button {
        background-color: #4CAF50;
        color: white;
        padding: 10px 24px;
        border-radius: 8px;
        border: none;
        font-size: 18px;
        transition-duration: 0.4s;
    }
    .stButton>button:hover {
        background-color: #45a049;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True
)