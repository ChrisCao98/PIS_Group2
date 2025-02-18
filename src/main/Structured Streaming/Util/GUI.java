package Util;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
/**
 * The main purpose of this class is to convert an array of type byte into an image and then display it.
 */
public class GUI extends JFrame {
    private JLabel imageLabel;

    public GUI() {
        setTitle("Image Viewer");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        imageLabel = new JLabel();
        imageLabel.setPreferredSize(new Dimension(1800, 1000));
        add(imageLabel, BorderLayout.CENTER);

        pack();
        setVisible(true);
    }

    //hint: make sure the size of the picture
    public void displayImage(byte[] imageData) {
        SwingUtilities.invokeLater(() -> {
            try {
                // 将字节数组转换为图像
                ByteArrayInputStream inputStream = new ByteArrayInputStream(imageData);

                BufferedImage originalImage = ImageIO.read(inputStream);

                imageLabel.setIcon(new ImageIcon(originalImage));

                int labelWidth = imageLabel.getWidth();
                int labelHeight = imageLabel.getHeight();

                BufferedImage scaledImage = new BufferedImage(labelWidth, labelHeight, BufferedImage.TYPE_INT_ARGB);
                Graphics2D g2d = scaledImage.createGraphics();
                g2d.drawImage(originalImage, 0, 0, labelWidth, labelHeight, null);
                g2d.dispose();

                imageLabel.setIcon(new ImageIcon(scaledImage));

                pack();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    public static byte[] readImage(String filePath) {
        try {
            File imageFile = new File(filePath);
            BufferedImage bufferedImage = ImageIO.read(imageFile);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(bufferedImage, "jpg", outputStream);
            return outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    public static void main(String[] args) {
        // 创建图形化界面
        GUI gui = new GUI();

        byte[] imageData = readImage("/home/chriscao/IdeaProjects/kfaka_no_gui/src/main/resources/testImage/image.jpg");

        gui.displayImage(imageData);
    }


}

